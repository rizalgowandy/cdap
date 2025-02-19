/*
 * Copyright © 2014-2018 Cask Data, Inc.
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
package io.cdap.cdap.runtime;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Injector;
import io.cdap.cdap.AppWithAnonymousWorkflow;
import io.cdap.cdap.MissingMapReduceWorkflowApp;
import io.cdap.cdap.MissingSparkWorkflowApp;
import io.cdap.cdap.NonUniqueProgramsInWorkflowApp;
import io.cdap.cdap.NonUniqueProgramsInWorkflowWithForkApp;
import io.cdap.cdap.OneActionWorkflowApp;
import io.cdap.cdap.ScheduleAppWithMissingWorkflow;
import io.cdap.cdap.WorkflowApp;
import io.cdap.cdap.WorkflowSchedulesWithSameNameApp;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.test.XSlowTests;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.twill.common.Threads;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Category(XSlowTests.class)
public class WorkflowTest {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowTest.class);
  private static final int startDelaySecs = 1; // Time between persisting a program as STARTING and RUNNING

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Supplier<File> TEMP_FOLDER_SUPPLIER = () -> {
    try {
      return tmpFolder.newFolder();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  };

  private int sourceId;

  private void setStartAndRunning(Store store, ProgramId id, String pid, ArtifactId artifactId) {
    long startTime = RunIds.getTime(pid, TimeUnit.SECONDS);
    store.setProvisioning(id.run(pid), Collections.emptyMap(),
                          Collections.singletonMap(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName()),
                          AppFabricTestHelper.createSourceId(++sourceId), artifactId);
    store.setProvisioned(id.run(pid), 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(id.run(pid), null, ImmutableMap.of(), AppFabricTestHelper.createSourceId(++sourceId));
    store.setRunning(id.run(pid), startTime + startDelaySecs, null, AppFabricTestHelper.createSourceId(++sourceId));
  }


  @Test(timeout = 120 * 1000L)
  public void testWorkflow() throws Exception {
    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(WorkflowApp.class,
                                                                                         TEMP_FOLDER_SUPPLIER);
    final Injector injector = AppFabricTestHelper.getInjector();
    final ProgramDescriptor programDescriptor = Iterators.filter(
      app.getPrograms().iterator(), input -> input.getProgramId().getType() == ProgramType.WORKFLOW).next();

    String inputPath = createInput();
    String outputPath = new File(tmpFolder.newFolder(), "output").getAbsolutePath();
    BasicArguments userArgs = new BasicArguments(ImmutableMap.of("inputPath", inputPath, "outputPath", outputPath));
    final SettableFuture<String> completion = SettableFuture.create();
    final ProgramController controller = AppFabricTestHelper.submit(app,
                                                                    programDescriptor.getSpecification().getClassName(),
                                                                    userArgs, TEMP_FOLDER_SUPPLIER);
    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        LOG.info("Starting");
        ArtifactId artifactId =
          controller.getProgramRunId().getNamespaceId().artifact("test", "1.0").toApiArtifactId();
        setStartAndRunning(injector.getInstance(Store.class), controller.getProgramRunId().getParent(),
                           controller.getProgramRunId().getRun(), artifactId);
      }

      @Override
      public void completed() {
        LOG.info("Completed");
        completion.set("Completed");
      }

      @Override
      public void error(Throwable cause) {
        LOG.info("Error", cause);
        completion.setException(cause);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    completion.get();
  }

  @Test(timeout = 120 * 1000L)
  public void testBadInputInWorkflow() throws Exception {
    // try deploying app containing Workflow configured with non-existent MapReduce program
    try {
      AppFabricTestHelper.deployApplicationWithManager(MissingMapReduceWorkflowApp.class, TEMP_FOLDER_SUPPLIER);
      Assert.fail("Should have thrown Exception because MapReduce program is missing in the Application.");
    } catch (Exception ex) {
      Assert.assertEquals("MapReduce program 'SomeMapReduceProgram' is not configured with the Application.",
                          ex.getCause().getMessage());
    }

    // try deploying app containing Workflow configured with non-existent Spark program
    try {
      AppFabricTestHelper.deployApplicationWithManager(MissingSparkWorkflowApp.class, TEMP_FOLDER_SUPPLIER);
      Assert.fail("Should have thrown Exception because Spark program is missing in the Application.");
    } catch (Exception ex) {
      Assert.assertEquals("Spark program 'SomeSparkProgram' is not configured with the Application.",
                          ex.getCause().getMessage());
    }

    // try deploying app containing Workflow configured with multiple schedules with the same name
    try {
      AppFabricTestHelper.deployApplicationWithManager(WorkflowSchedulesWithSameNameApp.class, TEMP_FOLDER_SUPPLIER);
      Assert.fail("Should have thrown Exception because Workflow is configured with schedules having same name.");
    } catch (Exception ex) {
      Assert.assertEquals("Duplicate schedule name for schedule: 'DailySchedule'",
                          ex.getCause().getMessage());
    }

    // try deploying app containing a schedule for non existent workflow
    try {
      AppFabricTestHelper.deployApplicationWithManager(ScheduleAppWithMissingWorkflow.class, TEMP_FOLDER_SUPPLIER);
      Assert.fail("Should have thrown Exception because Schedule is configured for non existent Workflow.");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getCause().getMessage().contains("is not configured"));
    }

    // try deploying app containing anonymous workflow
    try {
      AppFabricTestHelper.deployApplicationWithManager(AppWithAnonymousWorkflow.class, TEMP_FOLDER_SUPPLIER);
      Assert.fail("Should have thrown Exception because Workflow does not have name.");
    } catch (Exception ex) {
      Assert.assertEquals("'' name is not an ID. ID should be non empty and can contain only characters A-Za-z0-9_-",
                          ex.getCause().getMessage());
    }

    // try deploying app containing workflow with non-unique programs
    try {
      AppFabricTestHelper.deployApplicationWithManager(NonUniqueProgramsInWorkflowApp.class,
          TEMP_FOLDER_SUPPLIER);
      Assert.fail(
          "Should have thrown Exception because 'NoOpMR' added multiple times in the workflow "
              + "'NonUniqueProgramsInWorkflow'.");
    } catch (Exception ex) {
      Assert.assertEquals("Node 'NoOpMR' already exists in workflow 'NonUniqueProgramsInWorkflow'.",
                          ex.getCause().getMessage());
    }

    // try deploying app containing workflow fork with non-unique programs
    try {
      AppFabricTestHelper.deployApplicationWithManager(NonUniqueProgramsInWorkflowWithForkApp.class,
          TEMP_FOLDER_SUPPLIER);
      Assert.fail(
          "Should have thrown Exception because 'MyTestPredicate' added multiple times in the workflow "
              + "'NonUniqueProgramsInWorkflowWithFork'");
    } catch (Exception ex) {
      Assert.assertEquals("Node 'MyTestPredicate' already exists in workflow 'NonUniqueProgramsInWorkflowWithFork'.",
                          ex.getCause().getMessage());
    }
  }

  @Test(timeout = 120 * 1000L)
  public void testOneActionWorkflow() throws Exception {
    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(OneActionWorkflowApp.class,
                                                                                         TEMP_FOLDER_SUPPLIER);
    final Injector injector = AppFabricTestHelper.getInjector();
    final ProgramDescriptor programDescriptor = Iterators.filter(
      app.getPrograms().iterator(), input -> input.getProgramId().getType() == ProgramType.WORKFLOW).next();

    final SettableFuture<String> completion = SettableFuture.create();
    final ProgramController controller = AppFabricTestHelper.submit(app,
                                                                    programDescriptor.getSpecification().getClassName(),
                                                                    new BasicArguments(), TEMP_FOLDER_SUPPLIER);
    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        LOG.info("Initializing");
        ArtifactId artifactId =
          controller.getProgramRunId().getNamespaceId().artifact("test", "1.0").toApiArtifactId();
        setStartAndRunning(injector.getInstance(Store.class), controller.getProgramRunId().getParent(),
                           controller.getProgramRunId().getRun(), artifactId);
      }

      @Override
      public void completed() {
        LOG.info("Completed");
        completion.set("Completed");
      }

      @Override
      public void error(Throwable cause) {
        LOG.info("Error", cause);
        completion.setException(cause);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    String run = completion.get();
    Assert.assertEquals("Completed", run);
  }

  private String createInput() throws IOException {
    File inputDir = tmpFolder.newFolder();

    File inputFile = new File(inputDir.getPath() + "/words.txt");
    inputFile.deleteOnExit();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile))) {
      writer.write("this text has");
      writer.newLine();
      writer.write("two words text inside");
    }

    return inputDir.getAbsolutePath();
  }

  @After
  public void tearDown() {
    AppFabricTestHelper.shutdown();
  }

}
