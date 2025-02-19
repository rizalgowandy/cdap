/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.credential;

/**
 * Exception thrown during identity validation.
 */
public class IdentityValidationException extends Exception {

  /**
   * Creates a new identity validation exception.
   *
   * @param cause The cause of identity validation failure.
   */
  public IdentityValidationException(Throwable cause) {
    super("Failed to validate identity", cause);
  }

  /**
   * Creates a new identity validation exception.
   *
   * @param message The message of identity validation failure.
   */
  public IdentityValidationException(String message) {
    super(message);
  }

  /**
   * Creates a new identity validation exception.
   *
   * @param message The message of identity validation failure.
   * @param cause   The cause of identity validation failure.
   */
  public IdentityValidationException(String message, Throwable cause) {
    super(message, cause);
  }
}
