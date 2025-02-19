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

package io.cdap.cdap.logging.serialize;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggerContextVO;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.logback.TestLoggingContext;
import io.cdap.cdap.logging.appender.LogMessage;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases for LoggingEvent.
 */
public class LoggingEventTest {

  @BeforeClass
  public static void setUpContext() {
    LoggingContextAccessor.setLoggingContext(new TestLoggingContext("TEST_ACCT_ID1", "TEST_APP_ID1", "RUN1",
                                                                    "INSTANCE1"));
  }

  @Test
  public void testSerialize() throws Exception {
    Logger logger = LoggerFactory.getLogger(LoggingEventTest.class);
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent(
      getClass().getName(), (ch.qos.logback.classic.Logger) logger, Level.ERROR, "Log message1", null,
      new Object[] {"arg1", "arg2", "100"});
    iLoggingEvent.setLoggerName("loggerName1");
    iLoggingEvent.setThreadName("threadName1");
    iLoggingEvent.setTimeStamp(1234567890L);
    iLoggingEvent.setLoggerContextRemoteView(new LoggerContextVO("loggerContextRemoteView",
                                                                 ImmutableMap.of("key1", "value1", "key2", "value2"),
                                                                 100000L));
    Map<String, String> mdcMap = Maps.newHashMap(ImmutableMap.of("mdck1", "mdcv1", "mdck2", "mdck2"));
    iLoggingEvent.setMDCPropertyMap(mdcMap);

    LoggingEventSerializer serializer = new LoggingEventSerializer();
    byte[] encoded = serializer.toBytes(new LogMessage(iLoggingEvent, LoggingContextAccessor.getLoggingContext()));

    ILoggingEvent decodedEvent = serializer.fromBytes(ByteBuffer.wrap(encoded));
    LoggingEventSerializerTest.assertLoggingEventEquals(iLoggingEvent, decodedEvent);
  }

  @Test
  public void testEmptySerialize() throws Exception {
    Logger logger = LoggerFactory.getLogger(LoggingEventTest.class);
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent(
      getClass().getName(), (ch.qos.logback.classic.Logger) logger, Level.ERROR, null, null, null);

    LoggingEventSerializer serializer = new LoggingEventSerializer();
    byte[] encoded = serializer.toBytes(new LogMessage(iLoggingEvent, LoggingContextAccessor.getLoggingContext()));

    ILoggingEvent decodedEvent = serializer.fromBytes(ByteBuffer.wrap(encoded));
    LoggingEventSerializerTest.assertLoggingEventEquals(iLoggingEvent, decodedEvent);
  }
}
