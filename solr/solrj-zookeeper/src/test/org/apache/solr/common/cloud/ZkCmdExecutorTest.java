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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

/**
 * Tests for {@link ZkCmdExecutor} session-expiry handling:
 *
 * <ul>
 *   <li>Without a ConnectionManager: {@code SessionExpiredException} propagates immediately
 *       (backward-compatible behaviour, preserves the contract tested by
 *       {@code ZkSolrClientTest.testZkCmdExecutor}).
 *   <li>With a ConnectionManager: {@code SessionExpiredException} waits for the new session and
 *       retries the operation so callers do not have to handle it themselves.
 * </ul>
 */
public class ZkCmdExecutorTest extends SolrTestCaseJ4 {

  // -------------------------------------------------------------------------
  // Backward-compatibility: without ConnectionManager, SessionExpired propagates immediately
  // -------------------------------------------------------------------------

  @Test
  public void testSessionExpiredPropagatesImmediatelyWithoutConnectionManager() {
    ZkCmdExecutor executor = new ZkCmdExecutor(5000);
    // No setConnectionManager() call → old behaviour must be preserved

    expectThrows(
        KeeperException.SessionExpiredException.class,
        () ->
            executor.retryOperation(
                () -> {
                  throw new KeeperException.SessionExpiredException();
                }));
  }

  /**
   * Mirrors the assertion in {@code ZkSolrClientTest.testZkCmdExecutor}: a mix of
   * {@code ConnectionLoss} retries followed by {@code SessionExpired} still surfaces the
   * {@code SessionExpiredException} when no ConnectionManager is configured.
   */
  @Test
  public void testSessionExpiredAfterConnectionLossRetries_NoConnectionManager() {
    int timeoutMs = 3000;
    ZkCmdExecutor executor = new ZkCmdExecutor(timeoutMs);

    long start = System.nanoTime();
    expectThrows(
        KeeperException.SessionExpiredException.class,
        () ->
            executor.retryOperation(
                () -> {
                  if (System.nanoTime() - start
                      > TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new KeeperException.SessionExpiredException();
                  }
                  throw new KeeperException.ConnectionLossException();
                }));
  }

  // -------------------------------------------------------------------------
  // New behaviour: with ConnectionManager, SessionExpired waits and retries
  // -------------------------------------------------------------------------

  /**
   * When a ConnectionManager is wired in and the ZK client is currently connected,
   * {@code waitForConnected} returns immediately and the retry succeeds on the next attempt.
   * This verifies the retry path without requiring a real session expiry event.
   */
  @Test
  public void testSessionExpiredRetriesAndSucceedsWithConnectionManager() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(server.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {

      ZkCmdExecutor executor = new ZkCmdExecutor(AbstractZkTestCase.TIMEOUT);
      executor.setConnectionManager(zkClient.getConnectionManager());

      AtomicInteger attempts = new AtomicInteger(0);

      // First call throws SessionExpired; second call succeeds.
      // waitForConnected returns immediately because the client is still connected.
      String result =
          executor.retryOperation(
              () -> {
                int n = attempts.incrementAndGet();
                if (n == 1) {
                  throw new KeeperException.SessionExpiredException();
                }
                return "success-on-attempt-" + n;
              });

      assertEquals("success-on-attempt-2", result);
      assertEquals("must have retried exactly once", 2, attempts.get());
    } finally {
      server.shutdown();
    }
  }

  /**
   * Verifies that after a real ZK session expiry the executor retries and succeeds once the
   * connection is re-established. This tests the full reconnect loop end-to-end.
   */
  @Test
  public void testSessionExpiredAfterRealExpiry_RetrySucceeds() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(server.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {

      zkClient.makePath("/test/expiry-retry", true);

      // Expire the session; ConnectionManager will reconnect in the background
      server.expire(zkClient.getZooKeeper().getSessionId());

      // Wait until ConnectionManager has reconnected
      long deadline = System.currentTimeMillis() + AbstractZkTestCase.TIMEOUT;
      while (!zkClient.getConnectionManager().isConnectedAndNotClosed()
          && System.currentTimeMillis() < deadline) {
        Thread.sleep(100);
      }
      assertTrue(
          "client should have reconnected within " + AbstractZkTestCase.TIMEOUT + "ms",
          zkClient.getConnectionManager().isConnectedAndNotClosed());

      // After reconnect, ordinary ZK operations must work — verify via ZkCmdExecutor
      ZkCmdExecutor executor = new ZkCmdExecutor(AbstractZkTestCase.TIMEOUT);
      executor.setConnectionManager(zkClient.getConnectionManager());

      boolean[] exists =
          new boolean[] {
            executor.retryOperation(() -> zkClient.getZooKeeper().exists("/test/expiry-retry", false) != null)
          };
      assertTrue("node should exist after reconnect", exists[0]);
    } finally {
      server.shutdown();
    }
  }
}
