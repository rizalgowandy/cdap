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

package io.cdap.cdap.api.workflow;

import io.cdap.cdap.api.customaction.CustomActionSpecification;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import javax.annotation.Nullable;

/**
 * Represents the ACTION node in the {@link Workflow}.
 */
public class WorkflowActionNode extends WorkflowNode {

  private final ScheduleProgramInfo program;
  private final CustomActionSpecification customActionSpecification;

  public WorkflowActionNode(String nodeId, ScheduleProgramInfo program) {
    super(nodeId, WorkflowNodeType.ACTION);
    this.program = program;
    this.customActionSpecification = null;
  }

  public WorkflowActionNode(String nodeId, CustomActionSpecification customActionSpecification) {
    super(nodeId, WorkflowNodeType.ACTION);
    this.program = new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
        customActionSpecification.getName());
    this.customActionSpecification = customActionSpecification;
  }

  /**
   * @return the program information associated with the {@link WorkflowNode}
   */
  public ScheduleProgramInfo getProgram() {
    return program;
  }

  /**
   * @return the {@link CustomActionSpecification} if this {@link WorkflowNode} represents the
   *     custom action, otherwise null is returned
   */
  @Nullable
  public CustomActionSpecification getCustomActionSpecification() {
    return customActionSpecification;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("WorkflowActionNode{");
    sb.append("nodeId=").append(nodeId);
    sb.append(", program=").append(program);
    sb.append(", customActionSpecification=").append(customActionSpecification);
    sb.append('}');
    return sb.toString();
  }
}
