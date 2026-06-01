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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeoutException;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.cloud.ConnectionManager.IsClosed;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkCmdExecutor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private long retryDelay = 1500L; // 1 second would match timeout, so 500 ms over for padding
  private int retryCount;
  private double timeouts;
  private IsClosed isClosed;
  /**
   * Optional reference to the ConnectionManager. When set, retryOperation will wait for a new ZK
   * session to be established before retrying after a SessionExpiredException, instead of
   * propagating immediately. Set via {@link #setConnectionManager(ConnectionManager)} after the
   * ConnectionManager is initialised.
   */
  private volatile ConnectionManager connectionManager;

  public ZkCmdExecutor(int timeoutms) {
    this(timeoutms, null);
  }

  /**
   * TODO: At this point, this should probably take a SolrZkClient in its constructor.
   *
   * @param timeoutms the client timeout for the ZooKeeper clients that will be used with this
   *     class.
   */
  public ZkCmdExecutor(int timeoutms, IsClosed isClosed) {
    timeouts = timeoutms / 1000.0;
    this.retryCount = Math.round(0.5f * ((float) Math.sqrt(8.0f * timeouts + 1.0f) - 1.0f)) + 1;
    this.isClosed = isClosed;
  }

  public void setConnectionManager(ConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }

  public long getRetryDelay() {
    return retryDelay;
  }

  public void setRetryDelay(long retryDelay) {
    this.retryDelay = retryDelay;
  }

  /** Perform the given operation, retrying if the connection fails */
  public <T> T retryOperation(ZkOperation<T> operation)
      throws KeeperException, InterruptedException {
    KeeperException exception = null;
    for (int i = 0; i < retryCount; i++) {
      try {
        if (log.isTraceEnabled()) {
          log.trace("Begin zookeeper operation {}, attempt={}", operation, i);
        }
        if (i > 0 && isClosed()) {
          throw new AlreadyClosedException();
        }
        return operation.execute();
      } catch (KeeperException.ConnectionLossException e) {
        if (exception == null) {
          exception = e;
        }
        if (Thread.currentThread().isInterrupted()) {
          Thread.currentThread().interrupt();
          throw new InterruptedException();
        }
        if (i != retryCount - 1) {
          retryDelay(i);
        }
      } catch (KeeperException.SessionExpiredException e) {
        // Retry only when a ConnectionManager is available to wait for the new session.
        // SolrZkClient.keeper is volatile and is swapped to the new ZooKeeper instance by
        // updateKeeper() once reconnection succeeds, so the retry lambda uses the fresh keeper.
        // Without a ConnectionManager there is no way to know when a new session is ready, so
        // propagate immediately to preserve the original behaviour (no silent spin-retry).
        ConnectionManager cm = connectionManager;
        if (cm == null) {
          throw e;
        }
        if (exception == null) {
          exception = e;
        }
        if (Thread.currentThread().isInterrupted()) {
          Thread.currentThread().interrupt();
          throw new InterruptedException();
        }
        if (i != retryCount - 1) {
          log.warn(
              "ZooKeeper session expired during operation (attempt {}), waiting for reconnect before retry",
              i + 1);
          try {
            // Block until the ConnectionManager has a live session again.
            // Use the session timeout as the upper bound so we don't block forever.
            long waitMs = (long) (timeouts * 1000);
            cm.waitForConnected(waitMs);
          } catch (TimeoutException te) {
            // Reconnect didn't finish in time; proceed to next retry anyway.
            log.warn("Timed out waiting for ZooKeeper reconnect, will retry operation", te);
          }
        }
      } finally {
        if (log.isTraceEnabled()) {
          log.trace("End zookeeper operation {}", operation);
        }
      }
    }
    throw exception;
  }

  private boolean isClosed() {
    return isClosed != null && isClosed.isClosed();
  }

  /**
   * Performs a retry delay if this is not the first attempt
   *
   * @param attemptCount the number of the attempts performed so far
   */
  protected void retryDelay(int attemptCount) throws InterruptedException {
    Thread.sleep((attemptCount + 1) * retryDelay);
  }
}
