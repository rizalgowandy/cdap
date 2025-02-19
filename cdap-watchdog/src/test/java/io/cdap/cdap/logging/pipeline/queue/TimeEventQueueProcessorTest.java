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

package io.cdap.cdap.logging.pipeline.queue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.logging.meta.Checkpoint;
import io.cdap.cdap.logging.pipeline.LogPipelineTestUtil;
import io.cdap.cdap.logging.pipeline.LogProcessorPipelineContext;
import io.cdap.cdap.logging.pipeline.MockAppender;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link TimeEventQueueProcessor}.
 */
public class TimeEventQueueProcessorTest {
  private static final MetricsContext NO_OP_METRICS_CONTEXT = new NoopMetricsContext();

  @Test
  public void test() throws Exception {
    LoggerContext loggerContext = LogPipelineTestUtil.createLoggerContext("WARN",
                                                                          ImmutableMap.of("test.logger", "INFO"),
                                                                          MockAppender.class.getName());
    LogProcessorPipelineContext context = new LogProcessorPipelineContext(CConfiguration.create(),
                                                                          "test", loggerContext, NO_OP_METRICS_CONTEXT,
                                                                          0);
    context.start();
    TimeEventQueueProcessor<TestOffset> processor = new TimeEventQueueProcessor<>(context, 50, 1,
                                                                                  ImmutableList.of(0));
    long now = System.currentTimeMillis();
    List<ILoggingEvent> events = ImmutableList.of(
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "1", now - 1000),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "3", now - 700),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "5", now - 500),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "2", now - 900),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.ERROR, "4", now - 600),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "6", now - 100));

    ProcessedEventMetadata<TestOffset> metadata = processor.process(0, new TransformingIterator(events.iterator()));
    // all 6 events should be processed. This is because when the buffer is full after 5 events, time event queue
    // processor should append existing buffered events and enqueue 6th event
    Assert.assertEquals(6, metadata.getTotalEventsProcessed());
    for (Map.Entry<Integer, Checkpoint<TestOffset>> entry : metadata.getCheckpoints().entrySet()) {
      Checkpoint<TestOffset> value = entry.getValue();
      // offset should be max offset processed so far
      Assert.assertEquals(6, value.getOffset().getOffset());
    }
  }

  /**
   * Offset for unit-test.
   */
  public static final class TestOffset implements Comparable<TestOffset> {
    private final long offset;

    public TestOffset(long offset) {
      this.offset = offset;
    }

    public long getOffset() {
      return offset;
    }

    @Override
    public int compareTo(TestOffset o) {
      return Long.compare(this.offset, o.offset);
    }
  }

  /**
   * Iterator for testing.
   */
  private final class TransformingIterator implements Iterator<ProcessorEvent<TestOffset>> {
    private final Iterator<ILoggingEvent> iterator;

    TransformingIterator(Iterator<ILoggingEvent> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
     return iterator.hasNext();
    }

    @Override
    public ProcessorEvent<TestOffset> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      ILoggingEvent event = iterator.next();
      return new ProcessorEvent<>(event, 10, new TestOffset(Long.parseLong(event.getMessage())));
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Delete not supported.");
    }
  }
}
