/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.workflow;

import io.cdap.cdap.api.mapreduce.MapReduce;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.api.workflow.WorkflowNodeState;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.ProgramType;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Factory for {@link ProgramWorkflowRunner} which returns the appropriate {@link
 * ProgramWorkflowRunner} depending upon the program from the {@link SchedulableProgramType}. It
 * acts as the single point for conditionally creating the needed {@link ProgramWorkflowRunner} for
 * programs. Currently we support {@link MapReduce} and {@link Spark} in Workflow (See {@link
 * SchedulableProgramType}.
 */
final class ProgramWorkflowRunnerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramWorkflowRunnerFactory.class);

  private final CConfiguration cConf;
  private final WorkflowSpecification workflowSpec;
  private final ProgramRunnerFactory programRunnerFactory;
  private final Program workflowProgram;
  private final ProgramOptions workflowProgramOptions;
  private final ProgramStateWriter programStateWriter;

  ProgramWorkflowRunnerFactory(CConfiguration cConf, WorkflowSpecification workflowSpec,
      ProgramRunnerFactory programRunnerFactory,
      Program workflowProgram, ProgramOptions workflowProgramOptions,
      ProgramStateWriter programStateWriter) {
    this.cConf = cConf;
    this.workflowSpec = workflowSpec;
    this.programRunnerFactory = programRunnerFactory;
    this.workflowProgram = workflowProgram;
    this.workflowProgramOptions = workflowProgramOptions;
    this.programStateWriter = programStateWriter;
  }

  /**
   * Gives the appropriate instance of {@link ProgramWorkflowRunner} depending upon the passed
   * programType
   *
   * @param programType the programType
   * @param token the {@link WorkflowToken}
   * @param nodeStates the map of node ids to node states
   * @return the appropriate concrete implementation of {@link ProgramWorkflowRunner} for the
   *     program
   */
  ProgramWorkflowRunner getProgramWorkflowRunner(SchedulableProgramType programType,
      WorkflowToken token,
      String nodeId, Map<String, WorkflowNodeState> nodeStates) {
    switch (programType) {
      case MAPREDUCE:
        return new DefaultProgramWorkflowRunner(cConf, workflowProgram, workflowProgramOptions,
            programRunnerFactory,
            workflowSpec, token, nodeId, nodeStates, ProgramType.MAPREDUCE,
            programStateWriter);
      case SPARK:
        return new DefaultProgramWorkflowRunner(cConf, workflowProgram, workflowProgramOptions,
            programRunnerFactory,
            workflowSpec, token, nodeId, nodeStates, ProgramType.SPARK,
            programStateWriter);
      default:
        LOG.debug("No workflow program runner found for this program");
    }
    return null;  // if no workflow program runner was found for this program
  }
}
