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

package io.cdap.cdap.etl.batch.mapreduce;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.batch.InputContext;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.mapreduce.MapReduceTaskContext;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.batch.BatchPhaseSpec;
import io.cdap.cdap.etl.batch.PipelinePluginInstantiator;
import io.cdap.cdap.etl.batch.connector.MultiConnectorFactory;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.Destroyables;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.SetMultimapCodec;
import io.cdap.cdap.etl.exec.PipeTransformExecutor;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Initializes a TransformExecutor and runs transforms. This is used in both the mapper and reducer
 * since they do mostly the same thing, except the mapper needs to write to an aggregator or to
 * sinks, whereas the reducer needs to read from an aggregator and write to sinks.
 *
 * @param <KEY> the type of key to send into the transform executor
 * @param <VALUE> the type of value to send into the transform executor
 */
public class TransformRunner<KEY, VALUE> {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
      .create();
  private final PipeTransformExecutor<KeyValue<KEY, VALUE>> transformExecutor;
  private final OutputWriter<Object, Object> outputWriter;

  public TransformRunner(MapReduceTaskContext<Object, Object> context,
      Metrics metrics) throws Exception {
    JobContext jobContext = context.getHadoopContext();
    Configuration hConf = jobContext.getConfiguration();

    // figure out whether we are writing to a single output or to multiple outputs
    Map<String, String> properties = context.getSpecification().getProperties();
    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID),
        BatchPhaseSpec.class);
    this.outputWriter = getSinkWriter(context, phaseSpec.getPhase(), hConf);

    // instantiate and initialize all transformations and setup the TransformExecutor
    PipelinePluginInstantiator pluginInstantiator =
        new PipelinePluginInstantiator(context, metrics, phaseSpec, new MultiConnectorFactory());
    // stage name -> runtime args for that stage
    Map<String, String> runtimeArgs = GSON.fromJson(
        hConf.get(ETLMapReduce.RUNTIME_ARGS_KEY), ETLMapReduce.RUNTIME_ARGS_TYPE);

    // input alias name -> stage name mapping
    Map<String, String> inputAliasToStage = GSON.fromJson(hConf.get(ETLMapReduce.INPUT_ALIAS_KEY),
        ETLMapReduce.INPUT_ALIAS_TYPE);
    InputContext inputContext = context.getInputContext();
    // inputContext can be null (in case of reducers)
    String sourceStage =
        (inputContext != null) ? inputAliasToStage.get(inputContext.getInputName()) : null;

    PipelinePhase phase = phaseSpec.getPhase();
    Set<StageSpec> reducers = phase.getStagesOfType(BatchAggregator.PLUGIN_TYPE,
        BatchJoiner.PLUGIN_TYPE);
    if (!reducers.isEmpty()) {
      String reducerName = reducers.iterator().next().getName();
      // if we're in the mapper, get the part of the pipeline starting from sources and ending at aggregator
      if (jobContext instanceof Mapper.Context) {
        phase = phase.subsetTo(ImmutableSet.of(reducerName));
      } else {
        // if we're in the reducer, get the part of the pipeline starting from the aggregator and ending at sinks
        phase = phase.subsetFrom(ImmutableSet.of(reducerName));
      }
    }

    MapReduceTransformExecutorFactory<KeyValue<KEY, VALUE>> transformExecutorFactory =
        new MapReduceTransformExecutorFactory<>(context, pluginInstantiator, metrics,
            new BasicArguments(context.getWorkflowToken(), runtimeArgs),
            sourceStage, phaseSpec.pipelineContainsCondition(), outputWriter);
    this.transformExecutor = transformExecutorFactory.create(phase);
  }

  // this is needed because we need to write to the context differently depending on the number of outputs
  private OutputWriter<Object, Object> getSinkWriter(MapReduceTaskContext<Object, Object> context,
      PipelinePhase pipelinePhase,
      Configuration hConf) {
    Set<StageSpec> reducers = pipelinePhase.getStagesOfType(BatchAggregator.PLUGIN_TYPE,
        BatchJoiner.PLUGIN_TYPE);
    JobContext hadoopContext = context.getHadoopContext();
    if (!reducers.isEmpty() && hadoopContext instanceof Mapper.Context) {
      return new SingleOutputWriter<>(context);
    }

    String sinkOutputsStr = hConf.get(ETLMapReduce.SINK_OUTPUTS_KEY);

    // should never happen, this is set in initialize
    Preconditions.checkNotNull(sinkOutputsStr, "Sink outputs not found in Hadoop conf.");
    Map<String, SinkOutput> sinkOutputs = GSON.fromJson(sinkOutputsStr,
        ETLMapReduce.SINK_OUTPUTS_TYPE);
    return hasSingleOutput(sinkOutputs)
        ? new SingleOutputWriter<>(context) : new MultiOutputWriter<>(context, sinkOutputs);
  }

  private boolean hasSingleOutput(Map<String, SinkOutput> sinkOutputs) {
    // if no error datasets, check if we have more than one sink
    Set<String> allOutputs = new HashSet<>();

    for (SinkOutput sinkOutput : sinkOutputs.values()) {
      allOutputs.addAll(sinkOutput.getSinkOutputs());
    }
    return allOutputs.size() == 1;
  }

  public void transform(KEY key, VALUE value) throws Exception {
    KeyValue<KEY, VALUE> input = new KeyValue<>(key, value);
    transformExecutor.runOneIteration(input);
  }

  public void destroy() {
    Destroyables.destroyQuietly(transformExecutor);
  }
}
