/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.cdap.common.runtime;

import io.cdap.cdap.common.logging.common.UncaughtExceptionHandler;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A abstract base class for bridging standard main method to method invoked through apache
 * commons-daemon jsvc.
 */
public abstract class DaemonMain {

  private static final Logger LOG = LoggerFactory.getLogger(DaemonMain.class);

  /**
   * The main method. It simply call methods in the same sequence as if the program is started by
   * jsvc.
   */
  protected void doMain(final String[] args) throws Exception {
    try {
      init(args);
    } catch (Throwable t) {
      LOG.error("Exception raised when calling init", t);
      try {
        destroy();
      } catch (Throwable t2) {
        LOG.error("Exception raised when calling destroy", t);
        t.addSuppressed(t2);
      }
      // Throw to terminate the main thread
      throw t;
    }

    CountDownLatch shutdownLatch = new CountDownLatch(1);
    AtomicBoolean terminated = new AtomicBoolean();
    Runnable terminateRunnable = () -> {
      if (!terminated.compareAndSet(false, true)) {
        return;
      }
      try {
        try {
          DaemonMain.this.stop();
        } finally {
          try {
            DaemonMain.this.destroy();
          } finally {
            shutdownLatch.countDown();
          }
        }
      } catch (Throwable t) {
        LOG.error("Exception when shutting down: " + t.getMessage(), t);
      }
    };
    Runtime.getRuntime().addShutdownHook(new Thread(terminateRunnable));
    try {
      start();
    } catch (Throwable t) {
      // Throw to terminate the main thread
      LOG.error("Exception raised when calling start", t);
      terminateRunnable.run();
      throw t;
    }

    // Set uncaught exception handler after startup, this is so that if startup throws exception then we
    // want it to be logged as error (the handler logs it as debug)
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
    shutdownLatch.await();
  }

  /**
   * Invoked by jsvc to initialize the program.
   */
  public abstract void init(String[] args) throws Exception;

  /**
   * Invoked by jsvc to start the program.
   */
  public abstract void start() throws Exception;

  /**
   * Invoked by jsvc to stop the program.
   */
  public abstract void stop();

  /**
   * Invoked by jsvc for resource cleanup.
   */
  public abstract void destroy();
}
