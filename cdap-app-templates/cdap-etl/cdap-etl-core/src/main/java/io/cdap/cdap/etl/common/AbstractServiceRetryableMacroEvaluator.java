/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.common;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ErrorUtils.ActionErrorPair;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.retry.RetryableException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Class which contains common logic about retry logic.
 */
abstract class AbstractServiceRetryableMacroEvaluator implements MacroEvaluator {

  private static final long TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(600);
  private static final long RETRY_BASE_DELAY_MILLIS = 200L;
  private static final long RETRY_MAX_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final double RETRY_DELAY_MULTIPLIER = 1.2d;
  private static final double RETRY_RANDOMIZE_FACTOR = 0.1d;

  private final String functionName;
  private final String serviceName;

  AbstractServiceRetryableMacroEvaluator(String serviceName, String functionName) {
    this.serviceName = serviceName;
    this.functionName = functionName;
  }

  @Override
  public String lookup(String property) throws InvalidMacroException {
    throw new InvalidMacroException("The '" + functionName
        + "' macro function doesn't support direct property lookup for property '"
        + property + "'", new ErrorCategory(ErrorCategoryEnum.MACROS,
        String.format("%s-%s", serviceName, functionName)));
  }

  @Override
  public String evaluate(String macroFunction, String... args) throws InvalidMacroException {
    if (!functionName.equals(macroFunction)) {
      // This shouldn't happen
      throw new InvalidMacroException(
          "Invalid function name " + macroFunction + ". Expecting " + functionName,
          new ErrorCategory(ErrorCategoryEnum.MACROS,
          String.format("%s-%s", serviceName, functionName)));
    }

    long delay = RETRY_BASE_DELAY_MILLIS;
    double minMultiplier =
        RETRY_DELAY_MULTIPLIER - RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    double maxMultiplier =
        RETRY_DELAY_MULTIPLIER + RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    Stopwatch stopWatch = new Stopwatch().start();
    int retryCount = 0;
    RetryableException retryableException = null;
    try {
      while (stopWatch.elapsedTime(TimeUnit.MILLISECONDS) < TIMEOUT_MILLIS) {
        try {
          retryCount++;
          return evaluateMacro(macroFunction, args);
        } catch (RetryableException e) {
          retryableException = e;
          TimeUnit.MILLISECONDS.sleep(delay);
          delay =
              (long) (delay * (minMultiplier + Math.random() * (maxMultiplier - minMultiplier
                  + 1)));
          delay = Math.min(delay, RETRY_MAX_DELAY_MILLIS);
        } catch (IOException | HttpResponseException e) {
          throw new InvalidMacroException(e, new ErrorCategory(ErrorCategoryEnum.MACROS,
          String.format("%s-%s", serviceName, functionName)));
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Thread interrupted while trying evaluate "
              + "the value for '"
              + functionName
              + "' with"
              + " args "
              + Arrays.asList(args),
          e);
    }

    if (retryableException == null) {
      throw new IllegalStateException(
          "Exception after "
              + retryCount
              + " times trying to evaluate the "
              + "value for '"
              + functionName
              + "' with "
              + "args "
              + Arrays.asList(args));
    }

    throw new IllegalStateException(
        "Exception after "
            + retryCount
            + " times trying to evaluate the "
            + "value for '"
            + functionName
            + "' with "
            + "args "
            + Arrays.asList(args),
        retryableException);
  }

  @Override
  public Map<String, String> evaluateMap(String macroFunction, String... args)
      throws InvalidMacroException {
    if (!functionName.equals(macroFunction)) {
      // This shouldn't happen
      throw new InvalidMacroException("Invalid function name " + macroFunction
          + ". Expecting " + functionName, new ErrorCategory(ErrorCategoryEnum.MACROS,
          String.format("%s-%s", serviceName, functionName)));
    }

    // Make call with exponential delay on failure retry.
    long delay = RETRY_BASE_DELAY_MILLIS;
    double minMultiplier = RETRY_DELAY_MULTIPLIER - RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    double maxMultiplier = RETRY_DELAY_MULTIPLIER + RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    Exception ex = null;
    Stopwatch stopWatch = new Stopwatch().start();
    try {
      while (stopWatch.elapsedTime(TimeUnit.MILLISECONDS) < TIMEOUT_MILLIS) {
        try {
          return evaluateMacroMap(macroFunction, args);
        } catch (RetryableException e) {
          ex = e;
          TimeUnit.MILLISECONDS.sleep(delay);
          delay = (long) (delay * (minMultiplier + Math.random() * (maxMultiplier - minMultiplier
              + 1)));
          delay = Math.min(delay, RETRY_MAX_DELAY_MILLIS);
        } catch (HttpResponseException e) {
          int responseCode = e.getResponseCode();
          ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(responseCode);
          String errorReason = String.format("Failed to call %s service with status '%s'. %s",
              serviceName, responseCode, pair.getCorrectiveAction());
          throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.MACROS,
                  String.format("%s-%s", serviceName, functionName)), errorReason, e.getMessage(),
              pair.getErrorType(), false, ErrorCodeType.HTTP, String.valueOf(responseCode), null,
              e);
        } catch (IOException e) {
          throw new RuntimeException("Failed to evaluate the macro function '" + functionName
              + "' with args " + Arrays.asList(args), e);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Thread interrupted while trying to evaluate the value for '" + functionName
              + "' with args " + Arrays.asList(args), e);
    }
    throw new IllegalStateException(
        "Timed out when trying to evaluate the value for '" + functionName
            + "' with args " + Arrays.asList(args) + " with exception: ",
        ex == null ? ex : ex.getCause());
  }

  protected String validateAndRetrieveContent(String serviceName,
      HttpURLConnection urlConn) throws HttpResponseException {
    if (urlConn == null) {
      throw new RetryableException(serviceName + " service is not available");
    }
    validateResponseCode(serviceName, urlConn);
    try (Reader reader = new InputStreamReader(urlConn.getInputStream(), StandardCharsets.UTF_8)) {
      return CharStreams.toString(reader);
    } catch (IOException e) {
      // IOExceptions are retryable for idempotent operations.
      throw new RetryableException(e);
    } finally {
      urlConn.disconnect();
    }
  }

  private void validateResponseCode(String serviceName, HttpURLConnection urlConn)
      throws HttpResponseException {
    int responseCode;
    try {
      responseCode = urlConn.getResponseCode();
    } catch (IOException e) {
      // IOExceptions are retryable for idempotent operations.
      throw new RetryableException(e);
    }
    if (responseCode != HttpURLConnection.HTTP_OK) {
      if (HttpCodes.isRetryable(responseCode)) {
        throw new RetryableException(
            serviceName + " service is not available with status " + responseCode);
      }
      throw new HttpResponseException(
          "Failed to call " + serviceName + " service with status " + responseCode + ": "
              + getError(urlConn), responseCode);
    }
  }

  abstract Map<String, String> evaluateMacroMap(
      String macroFunction, String... args)
      throws InvalidMacroException, IOException, RetryableException, HttpResponseException;

  abstract String evaluateMacro(
      String macroFunction, String... args)
      throws InvalidMacroException, IOException, RetryableException;

  /**
   * Returns the full content of the error stream for the given {@link HttpURLConnection}.
   */
  private String getError(HttpURLConnection urlConn) {
    try (InputStream is = urlConn.getErrorStream()) {
      if (is == null) {
        return "Unknown error";
      }
      return new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "Unknown error due to failure to read from error output: " + e.getMessage();
    }
  }

  private static class HttpResponseException extends RuntimeException {
    private final int responseCode;

    private HttpResponseException(String message, int responseCode) {
      super(message);
      this.responseCode = responseCode;
    }

    int getResponseCode() {
      return responseCode;}
  }
}
