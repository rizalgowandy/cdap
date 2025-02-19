/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.gateway.handlers;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.http.HttpResponder;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogReader callback to encode log events, as {@link FormattedTextLogEvent} that contains log text
 * and log offset.
 */
public class TextOffsetCallback extends AbstractJSONCallback {

  private final PatternLayout patternLayout;
  private final boolean escape;

  /**
   * Simple Constructor for {@TextOffsetCallback}.
   */
  public TextOffsetCallback(HttpResponder responder, String logPattern, boolean escape) {
    super(responder);
    this.escape = escape;

    ch.qos.logback.classic.Logger rootLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext loggerContext = rootLogger.getLoggerContext();

    this.patternLayout = new PatternLayout();
    this.patternLayout.setContext(loggerContext);
    this.patternLayout.setPattern(logPattern);
  }

  @Override
  public void init() {
    super.init();
    patternLayout.start();
  }

  @Override
  public void close() {
    super.close();
    patternLayout.stop();
  }

  @Override
  protected Object encodeSend(LogEvent event) {
    String log = patternLayout.doLayout(event.getLoggingEvent());
    log = escape ? StringEscapeUtils.escapeHtml4(log) : log;
    return new FormattedTextLogEvent(log, event.getOffset());
  }
}
