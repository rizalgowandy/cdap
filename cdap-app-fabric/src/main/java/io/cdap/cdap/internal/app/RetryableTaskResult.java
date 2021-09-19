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
 */

package io.cdap.cdap.internal.app;

/**
 * RetryableTaskResult captures the result for RemoteTaskExecution
 */
public class RetryableTaskResult {

  private final byte[] result;
  private final int attempts;

  RetryableTaskResult(byte[] result, int attempts){
    this.result = result;
    this.attempts = attempts;
  }

  /**
   *
   * @return byte[] result of task
   */
  public byte[] getResult() {
    return result;
  }

  /**
   *
   * @return number of attempts for the task
   */
  public int getAttempts() {
    return attempts;
  }
}
