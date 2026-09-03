/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cluster.placement.plugins;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.Builders;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.ReplicaPlacement;
import org.apache.solr.cluster.placement.impl.PlacementRequestImpl;
import org.junit.Test;

/**
 * Tests the write-load balancing of {@link SimplePlacementFactory.SimplePlacementPlugin}: replicas
 * of write collections (members of {@code *_WRITE} aliases, or collections being created when the
 * mechanism is enabled) are placed to equalize the number of write replicas per node, giving a
 * bounded ramp-up for newly added empty nodes.
 */
public class WriteBalancePlacementTest extends AbstractPlacementFactoryTest {

  private static PlacementPlugin simplePlugin() {
    return new SimplePlacementFactory().createPluginInstance();
  }

  /** Places a new (not yet in the cluster state) collection with the given topology. */
  private static PlacementPlan placeNewCollection(
      Builders.ClusterBuilder clusterBuilder, String collectionName, int numShards, int rf)
      throws PlacementException, InterruptedException {
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(numShards, 0, 0, 0, List.of());
    SolrCollection solrCollection = collectionBuilder.build();
    return simplePlugin()
        .computePlacement(
            new PlacementRequestImpl(
                solrCollection,
                solrCollection.getShardNames(),
                new LinkedHashSet<>(clusterBuilder.buildLiveNodes()),
                rf,
                0,
                0),
            clusterBuilder.buildPlacementContext());
  }

  private static Map<Node, Integer> placementsPerNode(PlacementPlan placementPlan) {
    Map<Node, Integer> counts = new HashMap<>();
    for (ReplicaPlacement placement : placementPlan.getReplicaPlacements()) {
      counts.merge(placement.getNode(), 1, Integer::sum);
    }
    return counts;
  }

  private static void assertPlacementsOnNodeIndexes(
      List<Node> liveNodes, PlacementPlan plan, int... expectedNodeIndexes) {
    assertEquals(
        "unexpected number of placements",
        expectedNodeIndexes.length,
        plan.getReplicaPlacements().size());
    Map<Node, Integer> perNode = placementsPerNode(plan);
    Map<Node, Integer> expected = new HashMap<>();
    for (int nodeIndex : expectedNodeIndexes) {
      expected.merge(liveNodes.get(nodeIndex), 1, Integer::sum);
    }
    assertEquals("placements on unexpected nodes", expected, perNode);
  }

  /**
   * Adds a collection with one shard and two replicas on the given nodes, registered in a {@code
   * <name>_WRITE} alias.
   */
  private static void addWriteHead(
      Builders.ClusterBuilder clusterBuilder, String name, int nodeA, int nodeB) {
    Builders.CollectionBuilder builder = Builders.newCollectionBuilder(name);
    builder.customCollectionSetup(
        List.of(List.of("NRT " + nodeA, "NRT " + nodeB)), clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(builder);
    clusterBuilder.addAlias(name + "_WRITE", List.of(name));
  }

  /** Adds a plain (not aliased) collection with one shard and one replica on the given node. */
  private static void addFiller(
      Builders.ClusterBuilder clusterBuilder, String name, int nodeIndex, int replicaCount) {
    Builders.CollectionBuilder builder = Builders.newCollectionBuilder(name);
    List<String> replicas = new ArrayList<>();
    for (int i = 0; i < replicaCount; i++) {
      replicas.add("NRT " + nodeIndex);
    }
    // each list is one shard; spread replicas over distinct shards of the same node so that no
    // same-shard stack pre-exists on the node
    List<List<String>> shards = new ArrayList<>();
    for (String replica : replicas) {
      shards.add(List.of(replica));
    }
    builder.customCollectionSetup(shards, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(builder);
  }

  /**
   * A collection being created (absent from the cluster state) is a write collection: its replicas
   * avoid the nodes already hosting write heads even though those nodes host fewer replicas.
   */
  @Test
  public void testNewCollectionIsWriteBalancedAroundExistingHeads() throws Exception {
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(4);
    addWriteHead(clusterBuilder, "head1", 0, 1);
    addFiller(clusterBuilder, "filler", 3, 2);

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();
    PlacementPlan plan = placeNewCollection(clusterBuilder, "newHead", 1, 2);

    // write counts are [1, 1, 0, 0]: both replicas must avoid nodes 0 and 1
    assertPlacementsOnNodeIndexes(liveNodes, plan, 2, 3);
  }

  /** A collection that is only in a READ alias is read-only and does not count as write load. */
  @Test
  public void testCollectionInReadAliasOnlyIsNotWrite() throws Exception {
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(3);
    addWriteHead(clusterBuilder, "otherHead", 2, 2); // 1 replica on node 2, otherHead_WRITE exists
    // logsOld is only in the READ alias: 2 replicas on node 1 must not be counted as write load
    Builders.CollectionBuilder logsOld = Builders.newCollectionBuilder("logsOld");
    logsOld.customCollectionSetup(
        List.of(List.of("NRT 1"), List.of("NRT 1")), clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(logsOld);
    clusterBuilder.addAlias("logs_READ", List.of("logsOld"));
    addFiller(clusterBuilder, "filler", 0, 5);

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();
    PlacementPlan plan = placeNewCollection(clusterBuilder, "newHead", 1, 1);

    // If logsOld were (wrongly) counted as write, node 0 would win. Correctly, node 1 has no
    // write replicas and fewer total replicas than node 0.
    assertPlacementsOnNodeIndexes(liveNodes, plan, 1);
  }

  /**
   * Bounded ramp: a burst of new micro collections on a cluster with an empty node keeps write
   * replica counts balanced ({@code max - min <= 1} at all times); the empty node never overtakes
   * the others.
   */
  @Test
  public void testBoundedRampWithBurstOfMicroCollections() throws Exception {
    final int numNodes = 7;
    final int numBurst = 12;

    // names of placed collections with the two nodes their replicas went to
    List<String[]> placedHeads = new ArrayList<>();
    // write counts start with the three existing heads: one replica on each of nodes 0..5, node 6
    // (freshly added) empty
    int[] writeCounts = {1, 1, 1, 1, 1, 1, 0};

    for (int i = 0; i < numBurst; i++) {
      Builders.ClusterBuilder clusterBuilder =
          Builders.newClusterBuilder().initializeLiveNodes(numNodes);
      // three existing heads, one replica on each of nodes 0..5; node 6 is empty
      addWriteHead(clusterBuilder, "headA", 0, 1);
      addWriteHead(clusterBuilder, "headB", 2, 3);
      addWriteHead(clusterBuilder, "headC", 4, 5);
      // collections placed by previous iterations of the burst are write heads too
      for (String[] placed : placedHeads) {
        addWriteHead(
            clusterBuilder, placed[0], Integer.parseInt(placed[1]), Integer.parseInt(placed[2]));
      }

      List<Node> liveNodes = clusterBuilder.buildLiveNodes();
      PlacementPlan plan = placeNewCollection(clusterBuilder, "newHead" + i, 1, 2);
      assertEquals(2, plan.getReplicaPlacements().size());

      List<Node> placedNodes =
          plan.getReplicaPlacements().stream()
              .map(ReplicaPlacement::getNode)
              .sorted(Comparator.comparing(liveNodes::indexOf))
              .collect(Collectors.toList());
      assertEquals(
          "two replicas of the shard must be on distinct nodes",
          2,
          placedNodes.stream().distinct().count());

      for (Node node : placedNodes) {
        writeCounts[liveNodes.indexOf(node)]++;
      }
      placedHeads.add(
          new String[] {
            "newHead" + i,
            String.valueOf(liveNodes.indexOf(placedNodes.get(0))),
            String.valueOf(liveNodes.indexOf(placedNodes.get(1)))
          });

      int min = Integer.MAX_VALUE;
      int max = Integer.MIN_VALUE;
      for (int c : writeCounts) {
        min = Math.min(min, c);
        max = Math.max(max, c);
      }
      assertTrue(
          "write counts must stay balanced at all times (max - min <= 1) but were "
              + Arrays.toString(writeCounts),
          max - min <= 1);
    }

    // the initially empty node (index 6) must never carry more write replicas than the most
    // loaded of the original nodes
    int maxOnOriginalNodes = 0;
    for (int n = 0; n < 6; n++) {
      maxOnOriginalNodes = Math.max(maxOnOriginalNodes, writeCounts[n]);
    }
    assertTrue(
        "empty node overtook the loaded nodes: " + Arrays.toString(writeCounts),
        writeCounts[6] <= maxOnOriginalNodes);
  }

  /**
   * The RF=2 trap: with a large write counter skew (empty node vs loaded nodes), both replicas of a
   * shard still land on distinct nodes.
   */
  @Test
  public void testReplicasOfSameShardDoNotStackDuringRamp() throws Exception {
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(3);
    // three heads, each with one replica on node 0 and one on node 1 -> write counts [3, 3, 0]
    addWriteHead(clusterBuilder, "headA", 0, 1);
    addWriteHead(clusterBuilder, "headB", 0, 1);
    addWriteHead(clusterBuilder, "headC", 0, 1);

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();
    PlacementPlan plan = placeNewCollection(clusterBuilder, "newHead", 1, 2);

    Map<Node, Integer> perNode = placementsPerNode(plan);
    assertEquals(2, perNode.size()); // distinct nodes
    assertEquals(
        "the write-light node must receive one replica",
        Integer.valueOf(1),
        perNode.get(liveNodes.get(2)));
  }

  /** The per-collection maxReplicasPerNode limit wins over write balancing. */
  @Test
  public void testMaxReplicasPerNodeTakesPrecedenceOverWriteBalance() throws Exception {
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(3);
    // write counts [3, 3, 0]; node 1 carries an extra non-write replica so the second placement
    // deterministically prefers node 0
    addWriteHead(clusterBuilder, "headA", 0, 1);
    addWriteHead(clusterBuilder, "headB", 0, 1);
    addWriteHead(clusterBuilder, "headC", 0, 1);
    addFiller(clusterBuilder, "filler", 1, 1);

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder("limited");
    collectionBuilder.initializeShardsReplicas(2, 0, 0, 0, List.of());
    collectionBuilder.addCustomProperty(
        OrderedNodePlacementPlugin.MAX_REPLICAS_PER_NODE_PROPERTY, "1");
    SolrCollection solrCollection = collectionBuilder.build();

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();
    PlacementPlan plan =
        simplePlugin()
            .computePlacement(
                new PlacementRequestImpl(
                    solrCollection,
                    solrCollection.getShardNames(),
                    new LinkedHashSet<>(liveNodes),
                    1,
                    0,
                    0),
                clusterBuilder.buildPlacementContext());

    // shard 1 goes to the write-light node 2; shard 2 cannot join it on node 2 (limit of 1) and
    // goes to the lighter of the loaded nodes (node 0)
    assertPlacementsOnNodeIndexes(liveNodes, plan, 2, 0);
  }

  /** Replicas of read-only collections prefer write-light nodes without increasing write counts. */
  @Test
  public void testReadOnlyReplicaPlacementPrefersWriteLightNode() throws Exception {
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(3);
    // a write head with all its 3 replicas on node 1 -> write counts [0, 3, 0]
    Builders.CollectionBuilder logsHead = Builders.newCollectionBuilder("logsHead");
    logsHead.customCollectionSetup(
        List.of(List.of("NRT 1", "NRT 1", "NRT 1")), clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(logsHead);
    clusterBuilder.addAlias("logs_WRITE", List.of("logsHead"));
    // read-only collection with its only replica on node 2 (node 2 cannot take another replica
    // of the same shard)
    Builders.CollectionBuilder logsOld = Builders.newCollectionBuilder("logsOld");
    logsOld.customCollectionSetup(List.of(List.of("NRT 2")), clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(logsOld);
    clusterBuilder.addAlias("logs_READ", List.of("logsOld"));
    addFiller(clusterBuilder, "filler", 0, 5);

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();
    SolrCollection logsOldCollection = logsOld.build();
    PlacementPlan plan =
        simplePlugin()
            .computePlacement(
                new PlacementRequestImpl(
                    logsOldCollection,
                    logsOldCollection.getShardNames(),
                    new LinkedHashSet<>(liveNodes),
                    1,
                    0,
                    0),
                clusterBuilder.buildPlacementContext());

    // node 0 has more total replicas but no write replicas; without the write term in the node
    // weight the placement would go to node 1 (fewest replicas, but 3 write replicas)
    assertPlacementsOnNodeIndexes(liveNodes, plan, 0);
  }

  /**
   * Rotation self-healing: once a head leaves the {@code _WRITE} alias its replicas stop counting,
   * and the next head can return to the nodes hosting the rotated-out collection.
   */
  @Test
  public void testRotationReleasesWriteSlots() throws Exception {
    // Phase 1: headA (on nodes 0, 1) is the write head; nodes 2, 3 carry fillers
    Builders.ClusterBuilder phase1 = Builders.newClusterBuilder().initializeLiveNodes(4);
    Builders.CollectionBuilder headA1 = Builders.newCollectionBuilder("headA");
    headA1.customCollectionSetup(List.of(List.of("NRT 0", "NRT 1")), phase1.getLiveNodeBuilders());
    phase1.addCollection(headA1);
    phase1.addAlias("logs_WRITE", List.of("headA"));
    addFiller(phase1, "filler2", 2, 2);
    addFiller(phase1, "filler3", 3, 2);

    List<Node> liveNodes1 = phase1.buildLiveNodes();
    assertPlacementsOnNodeIndexes(liveNodes1, placeNewCollection(phase1, "headB", 1, 2), 2, 3);

    // Phase 2: rotation - headB (on nodes 2, 3) is now the write head, headA is read-only
    Builders.ClusterBuilder phase2 = Builders.newClusterBuilder().initializeLiveNodes(4);
    Builders.CollectionBuilder headA2 = Builders.newCollectionBuilder("headA");
    headA2.customCollectionSetup(List.of(List.of("NRT 0", "NRT 1")), phase2.getLiveNodeBuilders());
    phase2.addCollection(headA2);
    Builders.CollectionBuilder headB2 = Builders.newCollectionBuilder("headB");
    headB2.customCollectionSetup(List.of(List.of("NRT 2", "NRT 3")), phase2.getLiveNodeBuilders());
    phase2.addCollection(headB2);
    phase2.addAlias("logs_WRITE", List.of("headB"));
    phase2.addAlias("logs_READ", List.of("headA"));
    addFiller(phase2, "filler2", 2, 2);
    addFiller(phase2, "filler3", 3, 2);

    List<Node> liveNodes2 = phase2.buildLiveNodes();
    // write counts are now [0, 0, 1, 1] although nodes 2, 3 still host fewer... they host more
    // total replicas; the new head returns to nodes 0, 1 freed by the rotation
    assertPlacementsOnNodeIndexes(liveNodes2, placeNewCollection(phase2, "headC", 1, 2), 0, 1);
  }

  /** Without any _WRITE alias the plugin keeps its historical behavior. */
  @Test
  public void testNoWriteAliasesMeansLegacyBehavior() throws Exception {
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(2);
    // an existing collection with replicas on node 1 only; no aliases at all
    Builders.CollectionBuilder existing = Builders.newCollectionBuilder("existing");
    existing.customCollectionSetup(
        List.of(List.of("NRT 1"), List.of("NRT 1")), clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(existing);

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();
    PlacementPlan plan = placeNewCollection(clusterBuilder, "newCollection", 1, 2);

    // legacy Simple behavior for one RF=2 shard: the first replica goes to the node with the
    // fewest replicas (the empty node 0), the second one to the other node
    assertPlacementsOnNodeIndexes(liveNodes, plan, 0, 1);
  }

  /**
   * Without _WRITE aliases, degenerate clusters keep working exactly as before: a single node can
   * still host both replicas of an RF=2 shard (soft penalty instead of a hard rejection).
   */
  @Test
  public void testNoWriteAliasesSingleNodeCanStackShardReplicas() throws Exception {
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(1);

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();
    PlacementPlan plan = placeNewCollection(clusterBuilder, "newCollection", 1, 2);

    assertPlacementsOnNodeIndexes(liveNodes, plan, 0, 0);
  }
}
