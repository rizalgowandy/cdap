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

package io.cdap.cdap.api.exception;

import java.net.HttpURLConnection;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Utility class for handling errors and providing corrective actions.
 * <p>
 * This class provides a method to retrieve corrective actions and error types
 * based on HTTP status codes. It can be used to provide helpful error messages
 * to users.
 * </p>
 */
public final class ErrorUtils {

  /**
   * Pair class to hold corrective action and error type.
   */
  public static class ActionErrorPair {
    private final String correctiveAction;
    private final ErrorType errorType;

    ActionErrorPair(String correctiveAction, ErrorType errorType) {
      this.correctiveAction = correctiveAction;
      this.errorType = errorType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ActionErrorPair pair = (ActionErrorPair) o;
      return Objects.equals(correctiveAction, pair.correctiveAction)
        && Objects.equals(errorType, pair.errorType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(correctiveAction, errorType);
    }

    /**
     * Returns the corrective action for the error.
     *
     * @return The corrective action as a {@link String}.
     */
    public String getCorrectiveAction() {
      return correctiveAction;
    }

    /**
     * Returns the error type for the error.
     *
     * @return The error type as a {@link ErrorType}.
     */
    public ErrorType getErrorType() {
      return errorType;
    }
  }

  /**
   * Returns a {@link ActionErrorPair} object containing corrective action and error type based
   * on the given status code.
   *
   * @param statusCode The HTTP status code.
   * @return A ActionErrorPair object with corrective action and error type.
   */
  public static ActionErrorPair getActionErrorByStatusCode(Integer statusCode) {
    switch (statusCode) {
      case HttpURLConnection.HTTP_BAD_REQUEST:
        return new ActionErrorPair("Please check the request parameters and syntax.",
          ErrorType.USER);
      case HttpURLConnection.HTTP_UNAUTHORIZED:
        return new ActionErrorPair("Please ensure valid authentication "
          + "credentials are provided.", ErrorType.USER);
      case HttpURLConnection.HTTP_FORBIDDEN:
        return new ActionErrorPair("Please check you have permission to "
          + "access this resource.", ErrorType.USER);
      case HttpURLConnection.HTTP_NOT_FOUND:
        return new ActionErrorPair("Please verify the URL or resource you're "
          + "trying to access.", ErrorType.USER);
      case HttpURLConnection.HTTP_BAD_METHOD:
        return new ActionErrorPair("Please check if the HTTP method "
          + "(GET, POST, etc.) is correct.", ErrorType.USER);
      case HttpURLConnection.HTTP_CONFLICT:
        return new ActionErrorPair("Please resolve any conflicts, such as resource "
          + "versioning issues.", ErrorType.USER);
      case HttpURLConnection.HTTP_PRECON_FAILED:
        return new ActionErrorPair("Please check the request headers "
          + "to ensure the conditions are accurate and valid.", ErrorType.USER);
      case HttpURLConnection.HTTP_CLIENT_TIMEOUT:
        return new ActionErrorPair("The server took too long to respond, "
          + "please try again or check your connection.", ErrorType.USER);
      case 429:
        return new ActionErrorPair("Slow down your requests and "
          + "please try again later.", ErrorType.USER);
      case HttpURLConnection.HTTP_INTERNAL_ERROR:
        return new ActionErrorPair("The service is unavailable, "
          + "try again later.", ErrorType.SYSTEM);
      case HttpURLConnection.HTTP_UNAVAILABLE:
        return new ActionErrorPair("The service is unavailable, "
          + "please try again later.", ErrorType.SYSTEM);
      case HttpURLConnection.HTTP_BAD_GATEWAY:
        return new ActionErrorPair("Please ensure there are no network connectivity "
          + "issues between the proxy/gateway server and the upstream server or try again later.",
          ErrorType.SYSTEM);
      case HttpURLConnection.HTTP_PROXY_AUTH:
        return new ActionErrorPair("Proxy authentication required. Please check your "
            + "proxy settings and provide valid credentials.", ErrorType.USER);
      case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
        return new ActionErrorPair("Request cannot be processed in the requested format."
            + " Please check the Accept headers.", ErrorType.USER);
      case HttpURLConnection.HTTP_GONE:
        return new ActionErrorPair("Requested resource is no longer available.",
            ErrorType.USER);
      case HttpURLConnection.HTTP_LENGTH_REQUIRED:
        return new ActionErrorPair("Content-Length header is required. "
            + "Please include it in your request.", ErrorType.USER);
      case HttpURLConnection.HTTP_ENTITY_TOO_LARGE:
        return new ActionErrorPair("Request entity too large. "
            + "Please reduce payload size and try again.", ErrorType.USER);
      case HttpURLConnection.HTTP_REQ_TOO_LONG: // 414
        return new ActionErrorPair("Request URL is too long. "
            + "Consider shortening the URL.", ErrorType.USER);
      case HttpURLConnection.HTTP_UNSUPPORTED_TYPE:
        return new ActionErrorPair("Unsupported media type. "
            + "Please use a supported format and try again.", ErrorType.USER);
      case 416: // HTTP 416 Requested Range Not Satisfiable
        return new ActionErrorPair("Requested range is not satisfiable. "
            + "Please adjust range headers and try again.", ErrorType.USER);
      case 417: // HTTP 417 Expectation Failed
        return new ActionErrorPair("Expectation failed. "
            + "Server cannot meet Expect header requirements.", ErrorType.USER);
      case 421: // HTTP 421 Misdirected Request
        return new ActionErrorPair("Request was misdirected. "
            + "Please try sending it to the correct server.", ErrorType.USER);
      case 422: // HTTP 422 Unprocessable Entity
        return new ActionErrorPair("Request cannot be processed due to semantic errors. "
            + "Please check the request syntax and try again.", ErrorType.USER);
      case 423: // HTTP 423 Locked
        return new ActionErrorPair("Resource is locked. Please try again later.",
            ErrorType.USER);
      case 424: // HTTP 424 Failed Dependency
        return new ActionErrorPair("Request failed due to a failed dependency. "
            + "Please ensure related actions are successful.", ErrorType.USER);
      case 426: // HTTP 426 Upgrade Required
        return new ActionErrorPair("Upgrade required. "
            + "Please use a newer protocol version.", ErrorType.USER);
      case 428: // HTTP 428 Precondition Required
        return new ActionErrorPair("Request requires preconditions. "
            + "Please ensure headers are set correctly.", ErrorType.USER);
      case 431: // HTTP 431 Request Header Fields Too Large
        return new ActionErrorPair("Request headers are too large. "
            + "Please reduce the number or size of headers.", ErrorType.USER);
      case 451: // HTTP 451 Unavailable For Legal Reasons
        return new ActionErrorPair("Content is restricted due to legal reasons.", ErrorType.USER);
      default:
        return new ActionErrorPair(String.format("Request failed with error code: %s", statusCode),
          ErrorType.UNKNOWN);
    }
  }

  /**
   * Get a ProgramFailureException with the given error information.
   *
   * @param errorCategory The category of the error.
   * @param errorReason The reason for the error.
   * @param errorMessage The error message.
   * @param errorType The type of error.
   * @param dependency The bool representing whether the error is coming from a dependent service.
   * @param cause The cause of the error.
   * @return A ProgramFailureException with the given error information.
   */
  public static ProgramFailureException getProgramFailureException(ErrorCategory errorCategory,
    String errorReason, String errorMessage, ErrorType errorType, boolean dependency,
    @Nullable Throwable cause) {

    ProgramFailureException.Builder builder = new ProgramFailureException.Builder();

    if (cause != null) {
      builder = builder.withCause(cause);
    }

    return builder
      .withErrorCategory(errorCategory)
      .withErrorReason(errorReason)
      .withErrorMessage(errorMessage)
      .withErrorType(errorType)
      .withDependency(dependency)
      .build();
  }

  /**
   * Get a ProgramFailureException with the given error information.
   *
   * @param errorCategory The category of the error.
   * @param errorReason The reason for the error.
   * @param errorMessage The error message.
   * @param errorType The type of error.
   * @param dependency The bool representing whether the error is coming from a dependent service.
   * @param errorCodeType The string representing
   * @param errorCode The int representing error code.
   * @param supportedDocumentationUrl The URL to the supported documentation.
   * @param cause The cause of the error.
   * @return A ProgramFailureException with the given error information.
   */
  public static ProgramFailureException getProgramFailureException(ErrorCategory errorCategory,
      String errorReason, String errorMessage, ErrorType errorType, boolean dependency,
      @Nullable ErrorCodeType errorCodeType, @Nullable String errorCode,
      @Nullable String supportedDocumentationUrl, @Nullable Throwable cause) {

    ProgramFailureException.Builder builder = new ProgramFailureException.Builder();

    if (cause != null) {
      builder = builder.withCause(cause);
    }

    return builder
        .withErrorCategory(errorCategory)
        .withErrorReason(errorReason)
        .withErrorMessage(errorMessage)
        .withErrorType(errorType)
        .withDependency(dependency)
        .withErrorCodeType(errorCodeType)
        .withErrorCode(errorCode)
        .withSupportedDocumentationUrl(supportedDocumentationUrl)
        .build();
  }
}
