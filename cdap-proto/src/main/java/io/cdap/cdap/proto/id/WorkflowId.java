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

package io.cdap.cdap.proto.id;

import io.cdap.cdap.proto.ProgramType;

/**
 * Uniquely identifies a workflow.
 */
public class WorkflowId extends ProgramId implements ParentedId<ApplicationId> {

  public WorkflowId(String namespace, String application, String program) {
    super(namespace, application, ProgramType.WORKFLOW, program);
  }

  public WorkflowId(String namespace, String application, String version, String program) {
    super(namespace, application, version, ProgramType.WORKFLOW, program);
  }

  public WorkflowId(ApplicationId appId, String program) {
    super(appId, ProgramType.WORKFLOW, program);
  }

  public static WorkflowId fromString(String string) {
    return EntityId.fromString(string, WorkflowId.class);
  }
}
