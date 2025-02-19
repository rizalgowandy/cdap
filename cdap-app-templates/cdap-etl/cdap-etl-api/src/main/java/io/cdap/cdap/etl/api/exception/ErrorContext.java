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

package io.cdap.cdap.etl.api.exception;

/**
 * Context for error details provider.
 *
 * <p>
 * This class provides the context for the error details provider.
 * </p>
 */
public class ErrorContext {
  private final ErrorPhase phase;

  public ErrorContext(ErrorPhase phase) {
    this.phase = phase;
  }

  public ErrorPhase getPhase() {
    return phase;
  }
}
