/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.client;

import io.cdap.cdap.client.app.FakeApp;
import io.cdap.cdap.client.app.FakeWorkflow;
import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.test.XSlowTests;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link io.cdap.cdap.client.ServiceClient}.
 */
@Category(XSlowTests.class)
@RunWith(Parameterized.class)
public class ScheduleClientTestRun extends ClientTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduleClientTestRun.class);

  private final NamespaceId namespace = NamespaceId.DEFAULT;
  private final ApplicationId app = namespace.app(FakeApp.NAME);
  private final WorkflowId workflow = app.workflow(FakeWorkflow.NAME);
  private final ScheduleId schedule;

  private ScheduleClient scheduleClient;
  private ApplicationClient appClient;

  public ScheduleClientTestRun(String scheduleName) {
    this.schedule = app.schedule(scheduleName);
  }

  @Parameterized.Parameters(name = "{index}: scheduleName = {0}")
  public static Collection<String[]> data() {
    Collection<String[]> params = new ArrayList<>();
    params.add(new String[] { "someSchedule" });
    params.add(new String[] { "some +-:?'` Schedule" });
    params.add(new String[] { "No.10 - 0014002 AND No.16 0015006" });
    return params;
  }

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    scheduleClient = new ScheduleClient(clientConfig);
    appClient.deploy(namespace, createAppJarFile(FakeApp.class));
  }

  @After
  public void tearDown() throws Throwable {
    try {
      appClient.deleteApp(app);
    } catch (Exception e) {
      LOG.error("Error deleting app {} during test cleanup.", e);
    }
  }

  @Test
  public void testAll() throws Exception {
    File appJar = createAppJarFile(FakeApp.class);
    // deploy the app with time schedule
    FakeApp.AppConfig config = new FakeApp.AppConfig(true, schedule.getSchedule(), null);
    appClient.deploy(namespace, appJar, config);
    List<ScheduleDetail> list = scheduleClient.listSchedules(workflow);
    Assert.assertEquals(1, list.size());

    ScheduleDetail timeSchedule = list.get(0);
    ProtoTrigger.TimeTrigger timeTrigger = (ProtoTrigger.TimeTrigger) timeSchedule.getTrigger();

    Assert.assertEquals(schedule.getSchedule(), timeSchedule.getName());

    Assert.assertEquals(FakeApp.SCHEDULE_CRON, timeTrigger.getCronExpression());

    String status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SUSPENDED", status);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SUSPENDED", status);

    scheduleClient.resume(schedule);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SCHEDULED", status);

    long startTime = System.currentTimeMillis();
    scheduleClient.suspend(schedule);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SUSPENDED", status);
    long endTime = System.currentTimeMillis() + 1;

    scheduleClient.reEnableSuspendedSchedules(NamespaceId.DEFAULT, startTime, endTime);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SCHEDULED", status);

    scheduleClient.suspend(schedule);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SUSPENDED", status);

    scheduleClient.resume(schedule);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SCHEDULED", status);

    scheduleClient.suspend(schedule);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SUSPENDED", status);

    scheduleClient.resume(schedule);
    List<ScheduledRuntime> scheduledRuntimes = scheduleClient.nextRuntimes(workflow);
    scheduleClient.suspend(schedule);
    Assert.assertEquals(1, scheduledRuntimes.size());
    // simply assert that its scheduled for some time in the future (or scheduled for now, but hasn't quite
    // executed yet
    Assert.assertTrue(scheduledRuntimes.get(0).getTime() >= System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1));

    try {
      scheduleClient.nextRuntimes(app.workflow("nonexistentWorkflow"));
      Assert.fail("Expected not to be able to retrieve next run times for a nonexistent workflow.");
    } catch (NotFoundException expected) {
      // expected
    }
  }

  @Test
  public void testScheduleChanges() throws Exception {
    File appJar = createAppJarFile(FakeApp.class);

    // deploy the app with time schedule
    FakeApp.AppConfig config = new FakeApp.AppConfig(true, schedule.getSchedule(), null);
    appClient.deploy(namespace, appJar, config);
    // now there should be one schedule
    List<ScheduleDetail> list = scheduleClient.listSchedules(workflow);
    Assert.assertEquals(1, list.size());

    // test updating the schedule cron
    config = new FakeApp.AppConfig(true, schedule.getSchedule(), "0 2 1 1 *");
    appClient.deploy(namespace, appJar, config);
    list = scheduleClient.listSchedules(workflow);
    Assert.assertEquals(1, list.size());
    ProtoTrigger.TimeTrigger trigger = (ProtoTrigger.TimeTrigger) list.get(0).getTrigger();
    Assert.assertEquals("0 2 1 1 *", trigger.getCronExpression());

    // re-deploy the app without time schedule
    config = new FakeApp.AppConfig(false, null, null);
    appClient.deploy(namespace, appJar, config);
    // now there should be no schedule
    list = scheduleClient.listSchedules(workflow);
    Assert.assertEquals(0, list.size());
  }
}
