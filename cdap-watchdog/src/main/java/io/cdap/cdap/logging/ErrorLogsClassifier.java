/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.common.conf.Constants.Logging;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.proto.ErrorClassificationResponse;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.common.Strings;

/**
 * Classifies error logs and returns {@link ErrorClassificationResponse}.
 * TODO -
 *  - Add rule based classification.
 *  - Handle cases when stage name is not present in the mdc.
 */
public class ErrorLogsClassifier {
  private static final Gson GSON = new Gson();

  /**
   * Classifies error logs and returns {@link ErrorClassificationResponse}.
   *
   * @param logIter Logs Iterator that can be closed.
   * @param responder The HttpResponder.
   */
  public void classify(CloseableIterator<LogEvent> logIter, HttpResponder responder) {
    Map<String, ErrorClassificationResponse> responseMap = new HashMap<>();
    while(logIter.hasNext()) {
      ILoggingEvent logEvent = logIter.next().getLoggingEvent();
      Map<String, String> mdc = logEvent.getMDCPropertyMap();
      if (!mdc.containsKey(Logging.TAG_FAILED_STAGE)) {
        continue;
      }
      String stageName = mdc.get(Logging.TAG_FAILED_STAGE);
      String errorMessage = null;
      if (responseMap.containsKey(stageName)) {
        continue;
      }
      IThrowableProxy throwableProxy = logEvent.getThrowableProxy();
      while (throwableProxy != null) {
        if (ProgramFailureException.class.getName().equals(throwableProxy.getClassName())) {
          errorMessage = throwableProxy.getMessage();
        }
        throwableProxy = throwableProxy.getCause();
      }
      if (!Strings.isNullOrEmpty(errorMessage)) {
        ErrorClassificationResponse classificationResponse =
            new ErrorClassificationResponse.Builder()
                .setStageName(stageName)
                .setErrorCategory(String.format("%s-'%s'",mdc.get(Logging.TAG_ERROR_CATEGORY),
                    stageName))
                .setErrorReason(mdc.get(Logging.TAG_ERROR_REASON))
                .setErrorMessage(errorMessage)
                .setErrorType(mdc.get(Logging.TAG_ERROR_TYPE))
                .setDependency(mdc.get(Logging.TAG_DEPENDENCY))
                .setErrorCodeType(mdc.get(Logging.TAG_ERROR_CODE_TYPE))
                .setErrorCode(mdc.get(Logging.TAG_ERROR_CODE))
                .setSupportedDocumentationUrl(mdc.get(Logging.TAG_SUPPORTED_DOC_URL))
                .build();
        responseMap.put(stageName, classificationResponse);
      }
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(responseMap.values()));
  }
}
