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

package io.cdap.cdap.etl.common.submit;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.etl.batch.DefaultAggregatorContext;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

/**
 * Creates DefaultAggregatorContexts.
 */
public class AggregatorContextProvider implements ContextProvider<DefaultAggregatorContext> {

  private final PipelineRuntime pipelineRuntime;
  private final StageSpec stageSpec;
  private final Admin admin;

  public AggregatorContextProvider(PipelineRuntime pipelineRuntime, StageSpec stageSpec,
      Admin admin) {
    this.pipelineRuntime = pipelineRuntime;
    this.stageSpec = stageSpec;
    this.admin = admin;
  }

  @Override
  public DefaultAggregatorContext getContext(DatasetContext datasetContext) {
    return new DefaultAggregatorContext(pipelineRuntime, stageSpec, datasetContext, admin);
  }
}
