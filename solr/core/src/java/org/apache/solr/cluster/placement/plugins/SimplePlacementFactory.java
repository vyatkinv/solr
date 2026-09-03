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
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;

/**
 * Factory for creating {@link SimplePlacementPlugin}, a placement plugin implementing the logic
 * from the old <code>LegacyAssignStrategy</code>. This chooses nodes with the fewest cores
 * (especially cores of the same collection).
 *
 * <p>See {@link SameCollWeightedNode} for information on how this PlacementFactory weights nodes.
 *
 * <p>See {@link AffinityPlacementFactory} for a more realistic example and documentation.
 */
public class SimplePlacementFactory
    implements PlacementPluginFactory<PlacementPluginFactory.NoConfig> {

  @Override
  public PlacementPlugin createPluginInstance() {
    return new SimplePlacementPlugin();
  }

  public static class SimplePlacementPlugin extends OrderedNodePlacementPlugin {

    @Override
    protected Map<Node, WeightedNode> getBaseWeightedNodes(
        PlacementContext placementContext,
        Set<Node> nodes,
        Iterable<SolrCollection> relevantCollections,
        boolean skipNodesWithErrors) {
      // Write-load balancing: a collection is a "write" collection (its replicas each carry a
      // unit of indexing write load) when it is a member of a standard collection alias ending
      // with WRITE_ALIAS_SUFFIX, or - when write-load balancing is enabled at all - when it does
      // not exist in the cluster state yet (a collection being created is always a write head;
      // after rotation the head leaves the *_WRITE alias and stops counting).
      // When the cluster has no *_WRITE alias, the mechanism is fully disabled and the weight
      // formula below degenerates to the historical behavior.
      Set<String> writeCollections = new HashSet<>();
      boolean hasWriteAlias = false;
      for (Map.Entry<String, List<String>> aliasEntry :
          placementContext.getCluster().getCollectionAliases().entrySet()) {
        if (aliasEntry.getKey().endsWith(WRITE_ALIAS_SUFFIX)) {
          hasWriteAlias = true;
          writeCollections.addAll(aliasEntry.getValue());
        }
      }
      final boolean writeBalancingEnabled = hasWriteAlias;

      final Predicate<String> isWriteCollection;
      if (writeBalancingEnabled) {
        Set<String> existingCollections = new HashSet<>();
        for (SolrCollection collection : placementContext.getCluster().collections()) {
          existingCollections.add(collection.getName());
        }
        final Set<String> writeCollectionsFinal = writeCollections;
        isWriteCollection =
            collectionName ->
                writeCollectionsFinal.contains(collectionName)
                    || !existingCollections.contains(collectionName);
      } else {
        isWriteCollection = collectionName -> false;
      }

      HashMap<Node, WeightedNode> nodeVsShardCount = new HashMap<>();

      for (Node n : nodes) {
        nodeVsShardCount.computeIfAbsent(
            n, node -> new SameCollWeightedNode(node, isWriteCollection, writeBalancingEnabled));
      }

      return nodeVsShardCount;
    }
  }

  /**
   * This implementation weights nodes according to how many replicas of the same collection and
   * shard reside on the node, and (when write-load balancing is enabled, see {@link
   * SimplePlacementPlugin}) according to the write load the node already carries.
   *
   * <p>The total weight of the SameCollWeightedNode is the sum of:
   *
   * <ul>
   *   <li>The number of replicas on the node
   *   <li>5 * for each collection, the sum of:
   *       <ul>
   *         <li>(the number of replicas for that collection - 1)^2
   *       </ul>
   *   <li>1000 * for each shard, the sum of:
   *       <ul>
   *         <li>(the number of replicas for that shard - 1)^2
   *       </ul>
   *   <li>{@link #WRITE_REPLICA_MULT} * the number of replicas of write collections (NRT or TLOG
   *       replicas only, PULL replicas carry no write load) on the node
   * </ul>
   *
   * <p>The count of overlapping replicas for collections/shards must be squared, since we want
   * higher values to be penalized more than lower values. If a node has 2 collections with 3
   * replicas each, it should be weighted less than a node with 1 collection that has 5 replicas
   * placed there. Without squaring, the weight for the first node would be 26, and the weight of
   * the second node would be 25. So node #2 would be weighted lower even though it is considered to
   * be violating the constraints more. When we square the overlapping replica counts, the weight of
   * the first node would be 46 and the weight of the second node would be 85. This is the preferred
   * order.
   *
   * <p>The "relevant" weight with a replica is the sum of:
   *
   * <ul>
   *   <li>The number of replicas on the node
   *   <li>5 * (the number of replicas on the node for that replica's collection - 1)
   *   <li>1000 * (the number of replicas on the node for that replica's shard - 1)
   *   <li>{@link #WRITE_REPLICA_MULT} if the replica is an NRT or TLOG replica of a write
   *       collection
   * </ul>
   *
   * <p>Multiple replicas of the same shard are permitted on the same Node only when write-load
   * balancing is disabled, and then with a heavy soft penalty (see {@link
   * #canAddReplica(Replica)}).
   */
  private static class SameCollWeightedNode extends OrderedNodePlacementPlugin.WeightedNode {
    private static final int SAME_COL_MULT = 5;

    /**
     * When write-load balancing is enabled this multiplier is dead code (kept for documentation):
     * {@link #canAddReplica(Replica)} then enforces at most one replica of a given shard per node,
     * like the other built-in plugins, because with a write weight difference large enough (empty
     * node ramping up) this soft penalty would be defeated and both replicas of an RF=2 shard would
     * stack on the write-light node. When write-load balancing is disabled (no {@code *_WRITE}
     * aliases), the historical soft-penalty behavior is preserved and this multiplier still
     * contributes to placement decisions.
     */
    private static final int SAME_SHARD_MULT = 1000;

    /**
     * Weight multiplier applied to each replica of a write collection on the node. Large enough to
     * dominate any plausible replica count or collection penalty difference so that placements
     * equalize write load first, and bounded scenarios (such as the same-shard penalty) are
     * enforced by hard constraints in {@link #canAddReplica(Replica)}.
     */
    private static final int WRITE_REPLICA_MULT = 10_000;

    public Map<String, Integer> collectionReplicas;
    public int totalWeight = 0;

    /** Number of NRT/TLOG replicas of write collections on this node. */
    private int writeReplicas = 0;

    private final Predicate<String> isWriteCollection;

    private final boolean writeBalancingEnabled;

    SameCollWeightedNode(
        Node node, Predicate<String> isWriteCollection, boolean writeBalancingEnabled) {
      super(node);
      this.collectionReplicas = new HashMap<>();
      this.isWriteCollection = isWriteCollection;
      this.writeBalancingEnabled = writeBalancingEnabled;
    }

    @Override
    public int calcWeight() {
      return totalWeight + writeReplicas * WRITE_REPLICA_MULT;
    }

    @Override
    public int calcRelevantWeightWithReplica(Replica replica) {
      // Don't add 1 to the individual replica Counts, because 1 is subtracted from each when
      // calculating weights.
      // So since 1 would be added to each for the new replica, we can just use the original number
      // to calculate the weights.
      int colReplicaCount =
          collectionReplicas.getOrDefault(replica.getShard().getCollection().getName(), 0);
      int shardReplicaCount = getReplicasForShardOnNode(replica.getShard()).size();
      int writeWeight = writeReplicas * WRITE_REPLICA_MULT;
      if (isWriteReplica(replica)) {
        writeWeight += WRITE_REPLICA_MULT;
      }
      return getAllReplicaCount()
          + 1
          + colReplicaCount * SAME_COL_MULT
          + shardReplicaCount * SAME_SHARD_MULT
          + writeWeight;
    }

    @Override
    public boolean canAddReplica(Replica replica) {
      if (writeBalancingEnabled) {
        // At most one replica of a given shard per node (and the collection's optional per-node
        // replica limit), consistent with the other built-in plugins: the dominant write weight
        // would defeat the historical soft penalty during new-node ramp-up (see SAME_SHARD_MULT
        // note).
        return super.canAddReplica(replica);
      }
      // Write-load balancing disabled (no *_WRITE aliases): keep the historical Simple behavior
      // of allowing multiple replicas of the same shard on one node with a heavy soft penalty, so
      // that degenerate clusters (e.g. a single node) keep working exactly as before.
      return withinMaxReplicasPerNode(replica);
    }

    @Override
    protected boolean addProjectedReplicaWeights(Replica replica) {
      int colReplicaCountWith =
          collectionReplicas.merge(replica.getShard().getCollection().getName(), 1, Integer::sum);
      int shardReplicaCountWith = getReplicasForShardOnNode(replica.getShard()).size();
      totalWeight +=
          addedWeightOfAdditionalReplica(colReplicaCountWith - 1, shardReplicaCountWith - 1);
      if (isWriteReplica(replica)) {
        writeReplicas++;
      }
      return false;
    }

    @Override
    protected void initReplicaWeights(Replica replica) {
      addProjectedReplicaWeights(replica);
    }

    @Override
    protected void removeProjectedReplicaWeights(Replica replica) {
      Integer colReplicaCountWithout =
          Optional.ofNullable(
                  collectionReplicas.computeIfPresent(
                      replica.getShard().getCollection().getName(), (k, v) -> v - 1))
              .orElse(0);
      int shardReplicaCountWithout = getReplicasForShardOnNode(replica.getShard()).size();
      totalWeight -=
          addedWeightOfAdditionalReplica(colReplicaCountWithout, shardReplicaCountWithout);
      if (isWriteReplica(replica)) {
        writeReplicas--;
      }
    }

    /** PULL replicas do not receive the indexing write stream and carry no write load. */
    private boolean isWriteReplica(Replica replica) {
      return replica.getType() != Replica.ReplicaType.PULL
          && isWriteCollection.test(replica.getShard().getCollection().getName());
    }

    private int addedWeightOfAdditionalReplica(
        int colReplicaCountWithout, int shardReplicaCountWithout) {
      int additionalWeight = 1;
      if (colReplicaCountWithout > 0) {
        // x * 2 - 1 === x^2 - (x - 1)^2
        additionalWeight += SAME_COL_MULT * (colReplicaCountWithout * 2 - 1);
      }
      if (shardReplicaCountWithout > 0) {
        // x * 2 - 1 === x^2 - (x - 1)^2
        additionalWeight += SAME_SHARD_MULT * (shardReplicaCountWithout * 2 - 1);
      }
      return additionalWeight;
    }
  }
}
