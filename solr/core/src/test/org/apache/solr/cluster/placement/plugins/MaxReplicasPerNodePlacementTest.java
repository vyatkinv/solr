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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.BalancePlan;
import org.apache.solr.cluster.placement.Builders;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.ReplicaPlacement;
import org.apache.solr.cluster.placement.impl.BalanceRequestImpl;
import org.apache.solr.cluster.placement.impl.PlacementRequestImpl;
import org.junit.Test;

/**
 * Tests the optional per-node replica limit ({@link
 * OrderedNodePlacementPlugin#MAX_REPLICAS_PER_NODE_PROPERTY} collection custom property) enforced
 * by {@link OrderedNodePlacementPlugin} for all built-in placement plugins.
 */
public class MaxReplicasPerNodePlacementTest extends AbstractPlacementFactoryTest {

  private static List<PlacementPluginFactory<?>> allFactories() {
    return List.of(
        new SimplePlacementFactory(),
        new MinimizeCoresPlacementFactory(),
        new RandomPlacementFactory(),
        new AffinityPlacementFactory());
  }

  private static List<PlacementPluginFactory<?>> weightBasedFactories() {
    return List.of(
        new SimplePlacementFactory(),
        new MinimizeCoresPlacementFactory(),
        new AffinityPlacementFactory());
  }

  private static Map<Node, Integer> placementsPerNode(PlacementPlan placementPlan) {
    Map<Node, Integer> counts = new HashMap<>();
    for (ReplicaPlacement placement : placementPlan.getReplicaPlacements()) {
      counts.merge(placement.getNode(), 1, Integer::sum);
    }
    return counts;
  }

  /**
   * Build a cluster of 4 nodes where nodes 1, 2 and 3 each hold 3 replicas of an existing
   * collection and node 0 is empty (mimicking a freshly added node).
   */
  private Builders.ClusterBuilder buildClusterWithEmptyNode() {
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(4);
    Builders.CollectionBuilder existingCollectionBuilder =
        Builders.newCollectionBuilder("existingCollection");
    existingCollectionBuilder.customCollectionSetup(
        List.of(
            List.of("NRT 1", "NRT 2", "NRT 3"), // shard 1
            List.of("NRT 1", "NRT 2", "NRT 3"), // shard 2
            List.of("NRT 1", "NRT 2", "NRT 3")), // shard 3
        clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(existingCollectionBuilder);
    return clusterBuilder;
  }

  /**
   * Without the limit, a purely count-based plugin piles all replicas of a new collection onto the
   * empty node. This is the baseline behavior that the limit is designed to constrain.
   */
  @Test
  public void testEmptyNodeAttractsAllPlacementsWithoutLimit() throws Exception {
    PlacementPlugin plugin = new MinimizeCoresPlacementFactory().createPluginInstance();
    Builders.ClusterBuilder clusterBuilder = buildClusterWithEmptyNode();
    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder("newCollection");
    collectionBuilder.initializeShardsReplicas(3, 0, 0, 0, List.of());
    SolrCollection solrCollection = collectionBuilder.build();

    PlacementPlan placementPlan =
        plugin.computePlacement(
            new PlacementRequestImpl(
                solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes), 1, 0, 0),
            clusterBuilder.buildPlacementContext());

    assertEquals(3, placementPlan.getReplicaPlacements().size());
    assertEquals(
        "All replicas of the new collection should pile up on the empty node",
        Integer.valueOf(3),
        placementsPerNode(placementPlan).get(liveNodes.get(0)));
  }

  /**
   * With {@code placement.maxReplicasPerNode=1} set on the new collection, no node (including the
   * empty one) may receive more than one of its replicas, for any of the built-in plugins.
   */
  @Test
  public void testLimitSpreadsPlacementsAcrossNodes() throws Exception {
    for (PlacementPluginFactory<?> factory : allFactories()) {
      PlacementPlugin plugin = factory.createPluginInstance();
      Builders.ClusterBuilder clusterBuilder = buildClusterWithEmptyNode();
      List<Node> liveNodes = clusterBuilder.buildLiveNodes();

      Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder("newCollection");
      collectionBuilder
          .initializeShardsReplicas(3, 0, 0, 0, List.of())
          .addCustomProperty(OrderedNodePlacementPlugin.MAX_REPLICAS_PER_NODE_PROPERTY, "1");
      SolrCollection solrCollection = collectionBuilder.build();

      PlacementPlan placementPlan =
          plugin.computePlacement(
              new PlacementRequestImpl(
                  solrCollection,
                  solrCollection.getShardNames(),
                  new HashSet<>(liveNodes),
                  1,
                  0,
                  0),
              clusterBuilder.buildPlacementContext());

      assertEquals(
          "Wrong number of placements for " + factory.getClass().getSimpleName(),
          3,
          placementPlan.getReplicaPlacements().size());
      placementsPerNode(placementPlan)
          .forEach(
              (node, count) ->
                  assertTrue(
                      factory.getClass().getSimpleName()
                          + " placed "
                          + count
                          + " replicas on node "
                          + node.getName()
                          + ", limit is 1",
                      count <= 1));
    }
  }

  /**
   * When the limit makes the request infeasible (more replicas than {@code limit * nodes}), the
   * placement must fail with a {@link PlacementException} that mentions the limit.
   */
  @Test
  public void testLimitMakesPlacementInfeasible() {
    for (PlacementPluginFactory<?> factory : allFactories()) {
      PlacementPlugin plugin = factory.createPluginInstance();
      Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(2);
      List<Node> liveNodes = clusterBuilder.buildLiveNodes();

      Builders.CollectionBuilder collectionBuilder =
          Builders.newCollectionBuilder("infeasibleCollection");
      collectionBuilder
          .initializeShardsReplicas(3, 0, 0, 0, List.of())
          .addCustomProperty(OrderedNodePlacementPlugin.MAX_REPLICAS_PER_NODE_PROPERTY, "1");
      SolrCollection solrCollection = collectionBuilder.build();

      PlacementException e =
          expectThrows(
              PlacementException.class,
              "Expected placement to fail for " + factory.getClass().getSimpleName(),
              () ->
                  plugin.computePlacement(
                      new PlacementRequestImpl(
                          solrCollection,
                          solrCollection.getShardNames(),
                          new HashSet<>(liveNodes),
                          1,
                          0,
                          0),
                      clusterBuilder.buildPlacementContext()));
      assertTrue(
          "Exception message should mention the limit property, but was: " + e.getMessage(),
          e.getMessage().contains(OrderedNodePlacementPlugin.MAX_REPLICAS_PER_NODE_PROPERTY));
    }
  }

  /**
   * Balancing must respect the limit too: an over-loaded node cannot shed replicas onto a node that
   * has already reached the collection's per-node limit.
   */
  @Test
  public void testBalancingRespectsLimit() throws Exception {
    for (PlacementPluginFactory<?> factory : weightBasedFactories()) {
      PlacementPlugin plugin = factory.createPluginInstance();
      Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(2);

      // 6 single-replica shards, all on node 0; without a limit, balancing would move 3 to node 1
      Builders.CollectionBuilder collectionBuilder =
          Builders.newCollectionBuilder("unbalancedCollection");
      collectionBuilder
          .customCollectionSetup(
              List.of(
                  List.of("NRT 0"),
                  List.of("NRT 0"),
                  List.of("NRT 0"),
                  List.of("NRT 0"),
                  List.of("NRT 0"),
                  List.of("NRT 0")),
              clusterBuilder.getLiveNodeBuilders())
          .addCustomProperty(OrderedNodePlacementPlugin.MAX_REPLICAS_PER_NODE_PROPERTY, "2");
      clusterBuilder.addCollection(collectionBuilder);

      List<Node> liveNodes = clusterBuilder.buildLiveNodes();
      BalancePlan balancePlan =
          plugin.computeBalancing(
              new BalanceRequestImpl(new HashSet<>(liveNodes)),
              clusterBuilder.buildPlacementContext());

      Map<org.apache.solr.cluster.Replica, Node> movements = balancePlan.getReplicaMovements();
      assertEquals(
          "Balancing with "
              + factory.getClass().getSimpleName()
              + " should only move replicas up to the per-node limit, moved: "
              + movements,
          2,
          movements.size());
      movements.forEach(
          (replica, node) ->
              assertEquals("All movements should target the empty node", liveNodes.get(1), node));
    }
  }

  /** Invalid, blank and non-positive property values must be ignored (treated as "no limit"). */
  @Test
  public void testInvalidLimitValuesAreIgnored() throws Exception {
    for (String propertyValue : List.of("notANumber", " ", "0", "-1")) {
      for (PlacementPluginFactory<?> factory : allFactories()) {
        PlacementPlugin plugin = factory.createPluginInstance();
        Builders.ClusterBuilder clusterBuilder =
            Builders.newClusterBuilder().initializeLiveNodes(1);
        List<Node> liveNodes = clusterBuilder.buildLiveNodes();

        Builders.CollectionBuilder collectionBuilder =
            Builders.newCollectionBuilder("lenientCollection");
        collectionBuilder
            .initializeShardsReplicas(2, 0, 0, 0, List.of())
            .addCustomProperty(
                OrderedNodePlacementPlugin.MAX_REPLICAS_PER_NODE_PROPERTY, propertyValue);
        SolrCollection solrCollection = collectionBuilder.build();

        // Both single-replica shards land on the only node; an enforced limit of 1 (or a parse
        // failure) would have made this fail
        PlacementPlan placementPlan =
            plugin.computePlacement(
                new PlacementRequestImpl(
                    solrCollection,
                    solrCollection.getShardNames(),
                    new HashSet<>(liveNodes),
                    1,
                    0,
                    0),
                clusterBuilder.buildPlacementContext());
        assertEquals(
            "Placement with ignored limit value '"
                + propertyValue
                + "' failed for "
                + factory.getClass().getSimpleName(),
            2,
            placementPlan.getReplicaPlacements().size());
      }
    }
  }
}
