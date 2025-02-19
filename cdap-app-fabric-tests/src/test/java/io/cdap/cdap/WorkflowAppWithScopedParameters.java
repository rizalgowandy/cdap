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

package io.cdap.cdap;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.InputField;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.JavaSparkMain;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import io.cdap.cdap.api.workflow.NodeStatus;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.api.workflow.WorkflowNodeState;
import io.cdap.cdap.internal.app.runtime.batch.WordCount;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 */
public class WorkflowAppWithScopedParameters extends AbstractApplication {

  public static final String APP_NAME = "WorkflowAppWithScopedParameters";
  public static final String ONE_MR = "OneMR";
  public static final String ANOTHER_MR = "AnotherMR";
  public static final String ONE_SPARK = "OneSpark";
  public static final String ANOTHER_SPARK = "AnotherSpark";
  public static final String ONE_WORKFLOW = "OneWorkflow";
  public static final String ONE_ACTION = "OneAction";
  public static final String SUCCESS_TOKEN_KEY = "Success";
  public static final String SUCCESS_TOKEN_VALUE = "true";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("WorkflowApp which demonstrates the scoped runtime arguments.");
    addMapReduce(new OneMR());
    addMapReduce(new AnotherMR());
    addSpark(new OneSpark());
    addSpark(new AnotherSpark());
    addWorkflow(new OneWorkflow());
    createDataset("Purchase", KeyValueTable.class);
    createDataset("UserProfile", KeyValueTable.class);
  }

  /**
   *
   */
  public static class OneMR extends AbstractMapReduce {

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      context.record(getFieldLineageOperations());
      Map<String, String> args = context.getRuntimeArguments();

      Preconditions.checkArgument(context.getLogicalStartTime() == 1234567890000L);
      Preconditions.checkArgument(args.get("logical.start.time").equals("1234567890000"));
      Preconditions.checkArgument(args.get("input.path").contains("OneMRInput"));
      Preconditions.checkArgument(args.get("output.path").contains("OneMROutput"));

      String inputPath = args.get("input.path");
      String outputPath = args.get("output.path");
      WordCount.configureJob(context.getHadoopJob(), inputPath, outputPath);
    }

    public static Set<Operation> getFieldLineageOperations() {
      ReadOperation read = new ReadOperation("OneMRRead", "some description", EndPoint.of("ns", "source"),
                                             "field1", "field2", "field3");
      WriteOperation write = new WriteOperation("OneMRWrite", "some description", EndPoint.of("ns", "destination"),
                                                Arrays.asList(InputField.of("OneMRRead", "field1"),
                                                              InputField.of("OneMRRead", "field2")));
      return new HashSet<>(Arrays.asList(read, write));
    }
  }

  /**
   *
   */
  public static class AnotherMR extends AbstractMapReduce {

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Map<String, String> args = context.getRuntimeArguments();
      Preconditions.checkArgument(args.get("input.path").contains("AnotherMRInput"));
      Preconditions.checkArgument(args.get("output.path").contains("ProgramOutput"));

      String inputPath = args.get("input.path");
      String outputPath = args.get("output.path");
      WordCount.configureJob(context.getHadoopJob(), inputPath, outputPath);
    }
  }

  /**
   *
   */
  public static class OneSpark extends AbstractSpark {
    @Override
    public void initialize() throws Exception {
      SparkClientContext context = getContext();
      context.record(getFieldLineageOperations());

      Map<String, String> args = context.getRuntimeArguments();
      Preconditions.checkArgument(args.get("input.path").contains("SparkInput"));
      Preconditions.checkArgument(args.get("output.path").contains("ProgramOutput"));
    }

    public static Set<Operation> getFieldLineageOperations() {
      ReadOperation read = new ReadOperation("OneSparkRead", "some description", EndPoint.of("ns", "source"),
                                             "field1", "field2", "field3");
      WriteOperation write = new WriteOperation("OneSparkWrite", "some description", EndPoint.of("ns", "destination"),
                                                Arrays.asList(InputField.of("OneSparkRead", "field1"),
                                                              InputField.of("OneSparkRead", "field2")));
      return new HashSet<>(Arrays.asList(read, write));
    }

    @Override
    public void configure() {
      setMainClass(SparkTestProgram.class);
    }
  }

  /**
   *
   */
  public static class AnotherSpark extends AbstractSpark {
    @Override
    public void initialize() throws Exception {
      SparkClientContext context = getContext();
      Map<String, String> args = context.getRuntimeArguments();
      Preconditions.checkArgument(args.get("input.path").contains("SparkInput"));
      Preconditions.checkArgument(args.get("output.path").contains("AnotherSparkOutput"));
    }

    @Override
    public void configure() {
      setMainClass(SparkTestProgram.class);
    }
  }

  public static class SparkTestProgram implements JavaSparkMain {

    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      JavaSparkContext jsc = new JavaSparkContext();
      List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
      JavaRDD<Integer> distData = jsc.parallelize(data);
      distData.collect();
    }
  }

  /**
   *
   */
  public static class OneWorkflow extends AbstractWorkflow {

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      Preconditions.checkArgument(0 == context.getNodeStates().size());
    }

    @Override
    public void configure() {
      addMapReduce(ONE_MR);
      addSpark(ONE_SPARK);
      addAction(new OneAction());
      addMapReduce(ANOTHER_MR);
      addSpark(ANOTHER_SPARK);
    }

    @Override
    public void destroy() {
      Map<String, WorkflowNodeState> nodeStates = getContext().getNodeStates();
      Preconditions.checkArgument(5 == nodeStates.size());
      WorkflowNodeState nodeState = nodeStates.get(ONE_MR);
      Preconditions.checkArgument(ONE_MR.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(nodeState.getRunId() != null);
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());
      Preconditions.checkArgument(OneMR.getFieldLineageOperations().equals(nodeState.getFieldLineageOperations()));

      nodeState = nodeStates.get(ONE_SPARK);
      Preconditions.checkArgument(ONE_SPARK.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(nodeState.getRunId() != null);
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());
      Preconditions.checkArgument(OneSpark.getFieldLineageOperations().equals(nodeState.getFieldLineageOperations()));

      nodeState = nodeStates.get(ANOTHER_MR);
      Preconditions.checkArgument(ANOTHER_MR.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(nodeState.getRunId() != null);
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());
      Preconditions.checkArgument(0 == nodeState.getFieldLineageOperations().size());

      nodeState = nodeStates.get(ANOTHER_SPARK);
      Preconditions.checkArgument(ANOTHER_SPARK.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(nodeState.getRunId() != null);
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());
      Preconditions.checkArgument(0 == nodeState.getFieldLineageOperations().size());

      nodeState = nodeStates.get(ONE_ACTION);
      Preconditions.checkArgument(ONE_ACTION.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());
      Preconditions.checkArgument(0 == nodeState.getFieldLineageOperations().size());

      getContext().getToken().put(SUCCESS_TOKEN_KEY, SUCCESS_TOKEN_VALUE);
    }
  }

  public static class OneAction extends AbstractCustomAction {

    @Override
    public void run() {
      // no-op
    }
  }
}
