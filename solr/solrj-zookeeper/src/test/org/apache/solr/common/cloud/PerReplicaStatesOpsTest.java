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
package org.apache.solr.common.cloud;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.junit.Test;

/**
 * Tests for {@link PerReplicaStatesOps#persist(String, SolrZkClient)} session-expiry handling.
 *
 * <p>Before this fix {@code persist()} silently exited the retry loop when {@code multi()} threw
 * {@link KeeperException.SessionExpiredException} — the state write was silently lost. The fix
 * propagates the exception so callers can react (wait for reconnect, re-publish on next
 * onReconnect, etc.).
 */
public class PerReplicaStatesOpsTest extends SolrTestCaseJ4 {

  /**
   * Verifies that {@code SessionExpiredException} thrown by the underlying {@code multi()} call is
   * propagated by {@code persist()} rather than silently swallowed.
   *
   * <p>A minimal {@link SolrZkClient} subclass overrides {@link SolrZkClient#multi} to inject the
   * exception. Using try-with-resources ensures the ZK connection threads are always cleaned up.
   */
  @Test
  public void testPersistPropagatesSessionExpiredException() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();

    try {
      String collPath = "/collections/test_col";

      // Use a real client to set up the parent path that getChildren() in the error-log path needs
      try (SolrZkClient setupClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {
        setupClient.makePath(collPath, true);
        setupClient.create(
            collPath + "/state.json", "{}".getBytes(), CreateMode.PERSISTENT, true);
      }

      // Stub that overrides multi() to simulate SessionExpiredException.
      // Closed in finally so ZK threads don't leak.
      try (SessionExpiredMultiClient stubClient =
          new SessionExpiredMultiClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {

        PerReplicaStates prs =
            new PerReplicaStates(collPath + "/state.json", 0, Collections.emptyList());
        PerReplicaStatesOps ops =
            PerReplicaStatesOps.flipState("core_node1", State.ACTIVE, prs);

        expectThrows(
            KeeperException.SessionExpiredException.class,
            () -> ops.persist(collPath + "/state.json", stubClient));
      }
    } finally {
      server.shutdown();
    }
  }

  /**
   * Verifies that the stale-state retry ({@code NodeExistsException} / {@code NoNodeException})
   * still works correctly — our change must not break the existing retry path.
   */
  @Test
  public void testPersistCompletesSuccessfullyWithRealClient() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(server.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {

      String collPath = "/collections/retry_col";
      zkClient.makePath(collPath, true);
      zkClient.create(collPath + "/state.json", "{}".getBytes(), CreateMode.PERSISTENT, true);

      PerReplicaStates prs =
          new PerReplicaStates(collPath + "/state.json", 0, Collections.emptyList());
      PerReplicaStatesOps ops =
          PerReplicaStatesOps.addReplica("core_node1", State.ACTIVE, false, prs);

      // Must complete without throwing
      ops.persist(collPath + "/state.json", zkClient);

      List<String> children = zkClient.getChildren(collPath + "/state.json", null, true);
      assertFalse("PRS children should have been written by persist()", children.isEmpty());
    } finally {
      server.shutdown();
    }
  }

  // -------------------------------------------------------------------------
  // Stub: SolrZkClient that throws SessionExpiredException from multi()
  // -------------------------------------------------------------------------

  /**
   * Extends {@link SolrZkClient} to override {@link #multi} with a {@code SessionExpiredException}
   * injection. All other methods use the real implementation so that helper calls inside
   * {@code PerReplicaStatesOps} (e.g. {@code getChildren} in the error-log path) work correctly.
   *
   * <p>Must be closed after use to release the underlying ZooKeeper connection threads.
   */
  private static final class SessionExpiredMultiClient extends SolrZkClient {

    SessionExpiredMultiClient(String zkServerAddress, int zkClientTimeoutMs) {
      super(
          new SolrZkClient.Builder()
              .withUrl(zkServerAddress)
              .withTimeout(zkClientTimeoutMs, TimeUnit.MILLISECONDS));
    }

    @Override
    public List<OpResult> multi(Iterable<Op> ops, boolean retryOnConnLoss)
        throws InterruptedException, KeeperException {
      throw new KeeperException.SessionExpiredException();
    }
  }
}
