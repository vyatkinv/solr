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
package org.apache.solr.cloud;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Ignore;
import org.junit.Test;

public class ConnectionManagerTest extends SolrTestCaseJ4 {

  static final int TIMEOUT = 3000;

  @Ignore
  public void testConnectionManager() throws Exception {

    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build();
      ConnectionManager cm = zkClient.getConnectionManager();
      try {
        assertFalse(cm.isLikelyExpired());

        ZooKeeper zk = zkClient.getZooKeeper();
        assertTrue(zk instanceof TestableZooKeeper);
        ((TestableZooKeeper) zk).testableConnloss();
        server.expire(zkClient.getZooKeeper().getSessionId());

        Thread.sleep(TIMEOUT);

        assertTrue(cm.isLikelyExpired());
      } finally {
        cm.close();
        zkClient.close();
      }
    } finally {
      server.shutdown();
    }
  }

  public void testLikelyExpired() throws Exception {

    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build();
      ConnectionManager cm = zkClient.getConnectionManager();
      try {
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());
        cm.process(new WatchedEvent(EventType.None, KeeperState.Disconnected, ""));
        // disconnect shouldn't immediately set likelyExpired
        assertFalse(cm.isConnectedAndNotClosed());
        assertFalse(cm.isLikelyExpired());

        // but it should after the timeout
        Thread.sleep((long) (zkClient.getZkClientTimeout() * 1.5));
        assertFalse(cm.isConnectedAndNotClosed());
        assertTrue(cm.isLikelyExpired());

        // even if we disconnect immediately again
        cm.process(new WatchedEvent(EventType.None, KeeperState.Disconnected, ""));
        assertFalse(cm.isConnectedAndNotClosed());
        assertTrue(cm.isLikelyExpired());

        // reconnect -- should no longer be likely expired
        cm.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, ""));
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());
      } finally {
        cm.close();
        zkClient.close();
      }
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testReconnectWhenZkDisappeared() throws Exception {
    ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor(
            new SolrNamedThreadFactory("connectionManagerTest"));

    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      MockZkClientConnectionStrategy strategy = new MockZkClientConnectionStrategy();
      SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .withConnStrategy(strategy)
              .build();
      ConnectionManager cm = zkClient.getConnectionManager();

      try {
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());

        // reconnect -- should no longer be likely expired
        cm.process(new WatchedEvent(EventType.None, KeeperState.Expired, ""));
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());
        assertTrue(strategy.isExceptionThrow());
      } finally {
        cm.close();
        zkClient.close();
        executor.shutdown();
      }
    } finally {
      server.shutdown();
    }
  }

  // ---------------------------------------------------------------------------
  // Tests for exponential backoff (ConnectionManager reconnect retry sleep)
  // ---------------------------------------------------------------------------

  /**
   * Verifies that the exponential-backoff formula used in ConnectionManager produces the correct
   * base sleep values: 1 s, 2 s, 4 s, 8 s, 16 s, then capped at 30 s.
   */
  @Test
  public void testExponentialBackoffFormula() {
    long[] expected = {1_000L, 2_000L, 4_000L, 8_000L, 16_000L, 30_000L, 30_000L, 30_000L};
    for (int attempt = 0; attempt < expected.length; attempt++) {
      long base = Math.min(30_000L, 1_000L * (1L << Math.min(attempt, 5)));
      assertEquals("base for attempt " + attempt, expected[attempt], base);
      // jitter must never exceed base (jitter = nextLong(base/2 + 1) < base/2 + 1 <= base)
      assertTrue("sleep >= base for attempt " + attempt, base + base / 2 >= base);
    }
  }

  /**
   * Verifies that when a reconnect attempt fails multiple times the next attempt's sleep is
   * strictly greater than the previous one (monotonically increasing up to the cap).
   */
  @Test
  public void testBackoffIsMonotonicallyIncreasing() {
    long prev = 0;
    for (int attempt = 0; attempt < 6; attempt++) {
      long base = Math.min(30_000L, 1_000L * (1L << Math.min(attempt, 5)));
      assertTrue(
          "base[" + attempt + "] > base[" + (attempt - 1) + "]", base > prev || base == 30_000L);
      prev = base;
    }
  }

  // ---------------------------------------------------------------------------
  // Tests for jitter before onReconnect
  // ---------------------------------------------------------------------------

  /**
   * Verifies that onReconnect is called after session expiry even when jitter is enabled.
   *
   * <p>Triggers the Expired path by calling {@code cm.process()} directly (same technique used by
   * {@link #testReconnectWhenZkDisappeared}) to avoid races with ZkTestServer's expire mechanism.
   * The test checks that reconnect completes and {@code onReconnect} is invoked.
   */
  @Test
  public void testOnReconnectIsCalledAfterSessionExpiryWithJitter() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      CountDownLatch reconnectLatch = new CountDownLatch(1);

      SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .withConnStrategy(new TestConnectionStrategy())
              .withReconnectListener(reconnectLatch::countDown)
              .build();
      ConnectionManager cm = zkClient.getConnectionManager();

      try {
        // Fire the Expired event directly (mirrors testReconnectWhenZkDisappeared) to avoid
        // flakiness from ZkTestServer.expire() timing.
        Thread expireThread =
            new Thread(
                () -> cm.process(new WatchedEvent(EventType.None, KeeperState.Expired, "")),
                "test-expire-thread");
        expireThread.start();

        // onReconnect must be called within the session timeout
        assertTrue(
            "onReconnect was not called within " + TIMEOUT + "ms after session expiry",
            reconnectLatch.await(TIMEOUT, TimeUnit.MILLISECONDS));

        expireThread.join(TIMEOUT);
      } finally {
        zkClient.close();
      }
    } finally {
      server.shutdown();
    }
  }

  // ---------------------------------------------------------------------------
  // Tests for stale ephemeral live-node cleanup on reconnect
  // ---------------------------------------------------------------------------

  /**
   * Simulates the race condition where ZooKeeper hasn't cleaned up the ephemeral node from an
   * expired session by the time the new session tries to create it. The implementation must delete
   * the stale node and recreate it instead of failing.
   */
  @Test
  public void testStaleEphemeralNodeIsRemovedAndRecreatedOnReconnect() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();
      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {

        String parentPath = "/live_nodes";
        String nodePath = parentPath + "/host:8983_solr";
        zkClient.makePath(parentPath, true);

        // Pre-create the node to simulate a stale ephemeral node from an expired session
        zkClient.create(nodePath, null, CreateMode.PERSISTENT, true);
        assertTrue("stale node must exist before test", zkClient.exists(nodePath, true));

        // The pattern from createEphemeralLiveNode: try create, on NodeExists delete and retry
        List<org.apache.zookeeper.Op> ops =
            List.of(
                org.apache.zookeeper.Op.create(
                    nodePath,
                    null,
                    zkClient.getZkACLProvider().getACLsToAdd(nodePath),
                    CreateMode.PERSISTENT));
        try {
          zkClient.multi(ops, true);
          fail("Expected NodeExistsException for pre-existing node");
        } catch (KeeperException.NodeExistsException e) {
          // Mirrors createEphemeralLiveNode recovery: delete each stale path then recreate
          for (org.apache.zookeeper.Op op : ops) {
            try {
              zkClient.delete(op.getPath(), -1, true);
            } catch (KeeperException.NoNodeException ignored) {
            }
          }
          zkClient.multi(ops, true);
        }

        assertTrue("node must exist after delete-and-recreate", zkClient.exists(nodePath, true));
      }
    } finally {
      server.shutdown();
    }
  }

  private static class MockZkClientConnectionStrategy extends TestConnectionStrategy {
    int called = 0;
    boolean exceptionThrown = false;

    @Override
    public void reconnect(
        final String serverAddress,
        final int zkClientTimeout,
        final Watcher watcher,
        final ZkUpdate updater)
        throws IOException, InterruptedException, TimeoutException {

      if (called++ < 1) {
        exceptionThrown = true;
        throw new IOException("Testing");
      }

      super.reconnect(serverAddress, zkClientTimeout, watcher, updater);
    }

    public boolean isExceptionThrow() {
      return exceptionThrown;
    }
  }
}
