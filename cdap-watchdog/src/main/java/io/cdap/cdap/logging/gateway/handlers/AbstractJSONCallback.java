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

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * LogReader callback that sends JSON objects.
 */
public abstract class AbstractJSONCallback extends AbstractChunkedCallback {

  protected static final Gson GSON = new Gson();
  private final AtomicBoolean started = new AtomicBoolean();

  AbstractJSONCallback(HttpResponder responder) {
    super(responder);
  }

  @Override
  protected HttpHeaders getResponseHeaders() {
    return new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
  }

  @Override
  public void writeFinal() throws IOException {
    // If an object has been sent already, then simply send closing bracket.
    // If nothing has been sent so far, then send '[]' indicating an empty list
    if (started.get()) {
      encodeSend(CharBuffer.wrap("]"), true);
    } else {
      encodeSend(CharBuffer.wrap("[]"), true);
    }
  }

  @Override
  public void handleEvent(LogEvent logEvent) {
    try {
      // If it is the first logEvent, send an opening bracket.
      // If it is not the first logEvent, send a , to indicate that it is the next element in the list of JSON objects
      if (started.compareAndSet(false, true)) {
        encodeSend(CharBuffer.wrap("["), false);
      } else {
        encodeSend(CharBuffer.wrap(","), false);
      }

      encodeSend(CharBuffer.wrap(GSON.toJson(encodeSend(logEvent))), false);
    } catch (IOException e) {
      // Just propagate the exception, the caller of this Callback should be handling it.
      throw Throwables.propagate(e);
    }
  }

  /**
   * Return a {@link Object} that will be serialized to a JSON string
   */
  protected abstract Object encodeSend(LogEvent logEvent);
}
