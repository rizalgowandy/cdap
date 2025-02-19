/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.common.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import java.util.Collections;
import java.util.Map;
import org.apache.twill.api.logging.LogEntry;
import org.slf4j.Marker;

/**
 * Adapter from {@link org.apache.twill.api.logging.LogEntry} to {@link
 * ch.qos.logback.classic.spi.ILoggingEvent}.
 */
final class TwillLogEntryAdapter implements ILoggingEvent {

  private final LogEntry entry;

  TwillLogEntryAdapter(LogEntry entry) {
    this.entry = entry;
  }

  @Override
  public String getThreadName() {
    return entry.getThreadName();
  }

  @Override
  public Level getLevel() {
    switch (entry.getLogLevel()) {
      case FATAL:
        return Level.ERROR;
      case ERROR:
        return Level.ERROR;
      case WARN:
        return Level.WARN;
      case INFO:
        return Level.INFO;
      case DEBUG:
        return Level.DEBUG;
      case TRACE:
        return Level.TRACE;
      default:
        return Level.INFO;
    }
  }

  @Override
  public String getMessage() {
    return entry.getMessage();
  }

  @Override
  public Object[] getArgumentArray() {
    return new Object[0];
  }

  @Override
  public String getFormattedMessage() {
    return entry.getMessage();
  }

  @Override
  public String getLoggerName() {
    return entry.getLoggerName();
  }

  @Override
  public LoggerContextVO getLoggerContextVO() {
    return null;
  }

  @Override
  public IThrowableProxy getThrowableProxy() {
    if (entry.getThrowable() == null) {
      return null;
    }

    return new TwillLogThrowableAdapter(entry.getThrowable());
  }

  @Override
  public StackTraceElement[] getCallerData() {
    StackTraceElement[] stackTraceElements = entry.getStackTraces();
    if (stackTraceElements.length == 0) {
      stackTraceElements = new StackTraceElement[1];
      StackTraceElement stackTraceElement =
          new StackTraceElement(entry.getSourceClassName(), entry.getSourceMethodName(),
              entry.getFileName(), entry.getLineNumber());
      stackTraceElements[0] = stackTraceElement;

    }
    return stackTraceElements;
  }

  @Override
  public boolean hasCallerData() {
    return entry.getThrowable() != null;
  }

  @Override
  public Marker getMarker() {
    return null;
  }

  @Override
  public Map<String, String> getMDCPropertyMap() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, String> getMdc() {
    return Collections.emptyMap();
  }

  @Override
  public long getTimeStamp() {
    return entry.getTimestamp();
  }

  @Override
  public void prepareForDeferredProcessing() {

  }
}
