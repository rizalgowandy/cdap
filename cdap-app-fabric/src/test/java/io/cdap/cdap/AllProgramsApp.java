/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap;

import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.ObjectMappedTable;
import io.cdap.cdap.api.dataset.lib.ObjectMappedTableProperties;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * App that contains all program types. Used to test Metadata store.
 */
public class AllProgramsApp extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(AllProgramsApp.class);

  public static final String NAME = "App";
  public static final String DESC = "Application which has everything";

  public static final String DATASET_NAME = "kvt";
  public static final String DATASET_NAME2 = "kvt2";
  public static final String DATASET_NAME3 = "kvt3";
  public static final String PLUGIN_DESCRIPTION = "test plugin";
  public static final String PLUGIN_NAME = "mytestplugin";
  public static final String PLUGIN_TYPE = "testplugin";
  public static final String SCHEDULE_NAME = "testschedule";
  public static final String SCHEDULE_DESCRIPTION = "EveryMinute";
  public static final String DS_WITH_SCHEMA_NAME = "dsWithSchema";

  @Override
  public void configure() {
    setName(NAME);
    setDescription(DESC);
    createDataset(DATASET_NAME, KeyValueTable.class,
                  DatasetProperties.builder().setDescription("test dataset").build());
    createDataset(DATASET_NAME2, KeyValueTable.class);
    createDataset(DATASET_NAME3, KeyValueTable.class);
    addMapReduce(new NoOpMR());
    addMapReduce(new NoOpMR2());
    addWorkflow(new NoOpWorkflow());
    addWorker(new NoOpWorker());
    addSpark(new NoOpSpark());
    addService(new NoOpService());
    schedule(buildSchedule(SCHEDULE_NAME, ProgramType.WORKFLOW, NoOpWorkflow.NAME)
               .setDescription(SCHEDULE_DESCRIPTION)
               .triggerByTime("* * * * *"));
    try {
      createDataset(DS_WITH_SCHEMA_NAME, ObjectMappedTable.class,
                    ObjectMappedTableProperties.builder()
                      .setType(DsSchema.class)
                      .setDescription("test object mapped table")
                      .build()
      );
    } catch (UnsupportedTypeException e) {
      // ignore for test
    }
  }

  @SuppressWarnings("unused")
  public static class DsSchema {
    String field1;
    int field2;
  }

  /**
   *
   */
  public static class NoOpMR extends AbstractMapReduce {
    public static final String NAME = "NoOpMR";

    @Override
    protected void configure() {
      setName(NAME);
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(NoOpMapper.class);
      job.setReducerClass(NoOpReducer.class);
      context.addInput(Input.ofDataset(DATASET_NAME));
      context.addOutput(Output.ofDataset(DATASET_NAME2));
    }
  }

  /**
   * Similar to {@link NoOpMR}.
   */
  public static class NoOpMR2 extends AbstractMapReduce {
    public static final String NAME = "NoOpMR2";

    @Override
    protected void configure() {
      setName(NAME);
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      context.addInput(Input.ofDataset(DATASET_NAME2));
      context.addOutput(Output.ofDataset(DATASET_NAME));
    }
  }

  public static class NoOpMapper extends Mapper<LongWritable, BytesWritable, Text, Text>
    implements ProgramLifecycle<MapReduceContext> {
    @Override
    protected void map(LongWritable key, BytesWritable value,
                       Context context) throws IOException, InterruptedException {
      Text output = new Text(value.copyBytes());
      context.write(output, output);
    }
    @Override
    public void initialize(MapReduceContext context) throws Exception {
      Object obj = context.newPluginInstance("mrid");
      Assert.assertEquals("value", obj.toString());
    }

    @Override
    public void destroy() {

    }
  }

  public static class NoOpReducer extends Reducer<Text, Text, byte[], byte[]> {
    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {
      for (Text value : values) {
        byte[] bytes = value.copyBytes();
        context.write(bytes, bytes);
      }
    }
  }

  /**
   *
   */
  public static class NoOpSpark extends AbstractSpark {
    public static final String NAME = "NoOpSpark";

    @Override
    protected void configure() {
      setName(NAME);
      setMainClass(NoOpSparkProgram.class);
    }
  }

  /**
   *
   */
  public static class NoOpSparkProgram  {
    // An empty class since in App-Fabric we don't have Spark dependency.
    // The intention of this class is to test various MDS and meta operation only without running the program
  }

  /**
   *
   */
  public static class NoOpWorkflow extends AbstractWorkflow {

    public static final String NAME = "NoOpWorkflow";

    @Override
    public void configure() {
      setName(NAME);
      setDescription("NoOp Workflow description");
      addAction(new NoOpAction());
      addMapReduce(NoOpMR.NAME);
    }
  }

  /**
   *
   */
  public static class NoOpAction extends AbstractCustomAction {

    @Override
    public void run() {

    }
  }

  /**
   *
   */
  public static class NoOpWorker extends AbstractWorker {

    public static final String NAME = "NoOpWorker";

    @Override
    public void configure() {
      setName(NAME);
    }

    @Override
    public void run() {
      // no-op
    }
  }

  /**
   *
   */
  public static class NoOpService extends AbstractService {

    public static final String NAME = "NoOpService";
    public static final String ENDPOINT = "no-op";

    @Override
    protected void configure() {
      addHandler(new NoOpHandler());
    }

    public class NoOpHandler extends AbstractHttpServiceHandler {

      @UseDataSet(DATASET_NAME)
      private KeyValueTable table;

      @Path(ENDPOINT)
      @GET
      public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
        LOG.info("Endpoint {} called in service {}", ENDPOINT, NAME);
        table = getContext().getDataset(DATASET_NAME);
        table.write("no-op-service", "no-op-service");
        responder.sendStatus(200);
      }
    }
  }

  public static class PConfig extends PluginConfig {
    private double y;
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name(PLUGIN_NAME)
  @Description(PLUGIN_DESCRIPTION)
  public static class AppPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }
}
