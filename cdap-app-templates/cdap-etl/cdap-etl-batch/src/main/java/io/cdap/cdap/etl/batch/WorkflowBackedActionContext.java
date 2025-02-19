/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch;

import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.api.workflow.WorkflowNodeState;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link BatchActionContext} within a pipeline.
 */
public class WorkflowBackedActionContext extends AbstractBatchContext implements
    BatchActionContext {

  private final WorkflowContext workflowContext;

  public WorkflowBackedActionContext(WorkflowContext workflowContext,
      PipelineRuntime pipelineRuntime,
      StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec, workflowContext, workflowContext.getAdmin());
    this.workflowContext = workflowContext;
  }

  @Override
  public WorkflowToken getToken() {
    return workflowContext.getToken();
  }

  @Override
  public Map<String, WorkflowNodeState> getNodeStates() {
    return workflowContext.getNodeStates();
  }

  @Override
  public boolean isSuccessful() {
    return workflowContext.getState().getStatus() == ProgramStatus.COMPLETED;
  }

  @Override
  public void record(List<FieldOperation> fieldOperations) {
    throw new UnsupportedOperationException("Lineage recording is not supported.");
  }
}
