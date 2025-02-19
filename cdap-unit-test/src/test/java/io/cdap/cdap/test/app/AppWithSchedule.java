/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package io.cdap.cdap.test.app;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.lib.ObjectStores;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import io.cdap.cdap.api.workflow.Value;
import io.cdap.cdap.api.workflow.WorkflowToken;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application with workflow scheduling.
 */
public class AppWithSchedule extends AbstractApplication {

  public static final String INPUT_NAME = "input";
  public static final String OUTPUT_NAME = "output";
  public static final String EVERY_HOUR_SCHEDULE = "EveryHourSchedule";
  public static final String EVERY_THREE_SECOND_SCHEDULE = "EveryThreeSecondSchedule";
  public static final String WORKFLOW_NAME = "SampleWorkflow";

  @Override
  public void configure() {
    try {
      setName("AppWithSchedule");
      setDescription("Sample application");
      ObjectStores.createObjectStore(getConfigurer(), INPUT_NAME, String.class);
      ObjectStores.createObjectStore(getConfigurer(), OUTPUT_NAME, String.class);
      addWorkflow(new SampleWorkflow());
      schedule(buildSchedule(EVERY_HOUR_SCHEDULE, ProgramType.WORKFLOW, WORKFLOW_NAME)
                 .triggerByTime("0 */1 * * *"));
      schedule(buildSchedule(EVERY_THREE_SECOND_SCHEDULE, ProgramType.WORKFLOW, WORKFLOW_NAME)
                 .triggerByTime("0/3 * * * * ?"));
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Sample workflow. Schedules a dummy MR job.
   */
  public static class SampleWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
        setDescription("SampleWorkflow description");
        addAction(new DummyAction());
    }
  }

  /**
   * DummyAction
   */
  public static class DummyAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(DummyAction.class);

    @Override
    public void initialize() throws Exception {
      WorkflowToken token = getContext().getWorkflowToken();
      token.put("running", Value.of(true));
      token.put("finished", Value.of(false));
    }

    @Override
    public void run() {
      LOG.info("Ran dummy action");
      try {
        TimeUnit.MILLISECONDS.sleep(500);
      } catch (InterruptedException e) {
        LOG.info("Interrupted");
      }
    }

    @Override
    public void destroy() {
      WorkflowToken token = getContext().getWorkflowToken();
      token.put("running", Value.of(false));
      token.put("finished", Value.of(true));
    }
  }
}
