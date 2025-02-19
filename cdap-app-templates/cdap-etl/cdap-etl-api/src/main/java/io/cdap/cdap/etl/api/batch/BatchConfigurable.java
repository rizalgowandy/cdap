/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.batch;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.SubmitterLifecycle;

/**
 * Base class for Batch run configuration methods.
 *
 * @param <T> batch execution context
 */
@Beta
public abstract class BatchConfigurable<T extends BatchContext> implements PipelineConfigurable,
    SubmitterLifecycle<T> {

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Prepare the Batch run. Used to configure the job before starting the run.
   *
   * @param context batch execution context
   * @throws Exception if there's an error during this method invocation
   */
  @Override
  public abstract void prepareRun(T context) throws Exception;

  /**
   * Invoked after the Batch run finishes. Used to perform any end of the run logic.
   *
   * @param succeeded defines the result of batch execution: true if run succeeded, false
   *     otherwise
   * @param context batch execution context
   */
  @Override
  public void onRunFinish(boolean succeeded, T context) {
    // no-op
  }
}
