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

package io.cdap.cdap.internal.operation;

/**
 * Interface representing runner for LRO.
 * A runner initiates the run and returns a {@link OperationController} for lifecycle management.
 */
public interface OperationRunner {

  /**
   * Run an operation in asynchronous mode.
   *
   * @param runDetail {@link OperationRunDetail} for the operation to be run
   */
  OperationController run(OperationRunDetail runDetail) throws IllegalStateException;
}
