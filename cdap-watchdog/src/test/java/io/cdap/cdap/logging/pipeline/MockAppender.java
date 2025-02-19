/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.pipeline;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.cdap.cdap.common.io.Syncable;
import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Appender for unit tests.
 */
public final class MockAppender extends AppenderBase<ILoggingEvent> implements Flushable, Syncable {

  private final AtomicInteger flushCount = new AtomicInteger();
  private Queue<ILoggingEvent> pending;
  private Queue<ILoggingEvent> persisted;

  @Override
  protected void append(ILoggingEvent event) {
    pending.add(event);
  }

  public Queue<ILoggingEvent> getEvents() {
    return persisted;
  }

  public int getFlushCount() {
    return flushCount.get();
  }

  @Override
  public void start() {
    persisted = new ConcurrentLinkedQueue<>();
    pending = new LinkedList<>();
    super.start();
  }

  @Override
  public void stop() {
    persisted = null;
    pending = null;
    super.stop();
  }

  @Override
  public void flush() throws IOException {
    flushCount.incrementAndGet();
  }

  @Override
  public void sync() throws IOException {
    persisted.addAll(pending);
    pending.clear();
  }
}
