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

package io.cdap.cdap.etl.api;

import io.cdap.cdap.api.annotation.Beta;

/**
 * Interface for stage that supports initialize call for resource setup.
 *
 * @param <T> execution context
 */
@Beta
public interface SubmitterLifecycle<T> {

  /**
   * Prepare the run. Used to configure the job before starting the run.
   *
   * @param context submitter context
   * @throws Exception if there's an error during this method invocation
   */
  void prepareRun(T context) throws Exception;

  /**
   * Invoked after the run finishes. Used to perform any end of the run logic.
   *
   * @param succeeded defines the result of batch execution: true if run succeeded, false
   *     otherwise
   * @param context submitter context
   */
  void onRunFinish(boolean succeeded, T context);
}
