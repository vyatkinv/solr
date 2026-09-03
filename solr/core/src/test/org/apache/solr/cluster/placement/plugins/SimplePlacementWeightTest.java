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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.placement.Builders;
import org.apache.solr.cluster.placement.PlacementContext;
import org.junit.Test;

/**
 * Tests that the weight incrementally maintained by the {@code SameCollWeightedNode} used by {@link
 * SimplePlacementFactory} for replicas present in the cluster state matches the documented weight
 * formula ({@code replicas + 5 * sum_c (c-1)^2 + 1000 * sum_s (s-1)^2}), in particular when a node
 * hosts several replicas of the same shard.
 *
 * <p>Regression test for a bug where the incremental same-shard penalty was computed from the
 * collection replica counter instead of the shard replica counter.
 */
public class SimplePlacementWeightTest extends SolrTestCaseJ4 {

  /**
   * Node 0 hosts two replicas of shard 1 and two replicas of shard 2 of the same collection.
   * Expected weight: 4 replicas + 5 * (4-1)^2 + 1000 * ((2-1)^2 + (2-1)^2) = 4 + 45 + 2000 = 2049.
   * With the bug, the incremental penalty for the second replica of shard 2 used the collection
   * counter (3) instead of the shard counter (1), giving a much larger weight.
   */
  @Test
  public void testIncrementalWeightMatchesFormulaWithSameShardStack() throws Exception {
    SimplePlacementFactory.SimplePlacementPlugin plugin =
        (SimplePlacementFactory.SimplePlacementPlugin)
            new SimplePlacementFactory().createPluginInstance();

    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(2);
    Builders.CollectionBuilder collectionBuilder =
        Builders.newCollectionBuilder("stackedCollection");
    collectionBuilder.customCollectionSetup(
        List.of(
            List.of("NRT 0", "NRT 0"), // shard 1, both replicas on node 0
            List.of("NRT 0", "NRT 0")), // shard 2, both replicas on node 0
        clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();
    PlacementContext placementContext = clusterBuilder.buildPlacementContext();

    Map<Node, OrderedNodePlacementPlugin.WeightedNode> weightedNodes =
        plugin.getWeightedNodes(placementContext, new HashSet<>(liveNodes), List.of(), false);

    assertEquals(
        "Incrementally accumulated weight of node 0 must match the weight formula",
        2049,
        weightedNodes.get(liveNodes.get(0)).calcWeight());
    assertEquals(
        "Weight of node 1 (no replicas) must be zero",
        0,
        weightedNodes.get(liveNodes.get(1)).calcWeight());
  }

  /**
   * No same-shard stacks, one replica of each of two shards of one collection plus one replica of
   * another collection on node 0. Expected: 3 + 5 * ((2-1)^2 + (1-1)^2) + 0 = 8.
   */
  @Test
  public void testIncrementalWeightMatchesFormulaWithoutStacking() throws Exception {
    SimplePlacementFactory.SimplePlacementPlugin plugin =
        (SimplePlacementFactory.SimplePlacementPlugin)
            new SimplePlacementFactory().createPluginInstance();

    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(2);
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder("coll1");
    collectionBuilder.customCollectionSetup(
        List.of(
            List.of("NRT 0"), // shard 1
            List.of("NRT 0")), // shard 2
        clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);
    Builders.CollectionBuilder collectionBuilder2 = Builders.newCollectionBuilder("coll2");
    collectionBuilder2.customCollectionSetup(
        List.of(List.of("NRT 0")), clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder2);

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();
    PlacementContext placementContext = clusterBuilder.buildPlacementContext();

    Set<Node> nodes = new HashSet<>(liveNodes);
    Map<Node, OrderedNodePlacementPlugin.WeightedNode> weightedNodes =
        plugin.getWeightedNodes(placementContext, nodes, List.of(), false);

    assertEquals(8, weightedNodes.get(liveNodes.get(0)).calcWeight());
    assertEquals(0, weightedNodes.get(liveNodes.get(1)).calcWeight());
  }
}
