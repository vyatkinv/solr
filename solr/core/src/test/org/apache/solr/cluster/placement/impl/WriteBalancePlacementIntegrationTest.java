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

package org.apache.solr.cluster.placement.impl;

import java.util.HashSet;
import java.util.Set;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration test (MiniSolrCloudCluster) for the write-load balancing of the default {@link
 * org.apache.solr.cluster.placement.plugins.SimplePlacementFactory.SimplePlacementPlugin}: when a
 * standard collection alias with the {@code _WRITE} suffix exists, replicas of newly created
 * collections are placed to equalize the number of write replicas per node (including through the
 * real Overseer CREATE path, i.e. the {@code wrapCloudManager} alias delegation chain).
 */
public class WriteBalancePlacementIntegrationTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4).addConfig("conf", configset("cloud-minimal")).configure();
  }

  private static Set<String> replicaNodes(DocCollection collection) {
    Set<String> nodes = new HashSet<>();
    collection.forEachReplica((shard, replica) -> nodes.add(replica.getNodeName()));
    return nodes;
  }

  /**
   * A new collection created while a {@code _WRITE} alias exists must be placed on the nodes with
   * the fewest write replicas, even if those nodes host more total replicas.
   */
  @Test
  public void testCreateBalancesWriteLoad() throws Exception {
    String node0 = cluster.getJettySolrRunner(0).getNodeName();
    String node1 = cluster.getJettySolrRunner(1).getNodeName();
    String node2 = cluster.getJettySolrRunner(2).getNodeName();
    String node3 = cluster.getJettySolrRunner(3).getNodeName();

    // a read-only filler collection with all its 8 replicas forced onto nodes 2 and 3
    CollectionAdminRequest.createCollection("filler", "conf", 4, 2)
        .setCreateNodeSet(node2 + "," + node3)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection("filler", 4, 8);

    // the current write head with its 2 replicas forced onto nodes 0 and 1
    CollectionAdminRequest.createCollection("logsA", "conf", 1, 2)
        .setCreateNodeSet(node0 + "," + node1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection("logsA", 1, 2);

    // rotation metadata: logsA is the write head. From now on write-load balancing is enabled.
    CollectionAdminRequest.createAlias("logs_WRITE", "logsA").process(cluster.getSolrClient());

    // create the new head without any node restriction: write replicas live only on nodes 0 and 1,
    // so write-load balancing must send both replicas of the new head to nodes 2 and 3 although
    // they already host 4 filler replicas each (a purely replica-count-driven placement would
    // choose nodes 0 and 1)
    CollectionAdminRequest.createCollection("logsB", "conf", 1, 2).process(cluster.getSolrClient());
    cluster.waitForActiveCollection("logsB", 1, 2);

    DocCollection logsB = getCollectionState("logsB");
    Set<String> logsBNodes = replicaNodes(logsB);
    assertEquals(
        "both replicas of the new head must be on distinct write-light nodes 2 and 3: " + logsB,
        Set.of(node2, node3),
        logsBNodes);

    // and the write head's replicas stayed where they were forced
    DocCollection logsA = getCollectionState("logsA");
    assertEquals(Set.of(node0, node1), replicaNodes(logsA));

    // replica types are NRT for all placed replicas (they carry write load)
    logsB.forEachReplica((shard, replica) -> assertEquals(Replica.Type.NRT, replica.getType()));
  }
}
