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
package io.cdap.cdap.etl.batch.customaction;

import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.customaction.CustomAction;
import io.cdap.cdap.api.customaction.CustomActionContext;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.cdap.etl.batch.BatchPhaseSpec;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.SetMultimapCodec;
import io.cdap.cdap.etl.common.plugin.PipelinePluginContext;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the {@link CustomAction} used in DataPipeline application.
 */
public class PipelineAction extends AbstractCustomAction {

  // This is only visible during the configure time, not at runtime.
  private final BatchPhaseSpec phaseSpec;
  private Metrics metrics;

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
      .create();

  public PipelineAction(BatchPhaseSpec phaseSpec) {
    this.phaseSpec = phaseSpec;
  }

  @Override
  protected void configure() {
    setName(phaseSpec.getPhaseName());
    setDescription("CustomAction phase executor. " + phaseSpec.getPhaseName());

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec));
    setProperties(properties);
  }

  @Override
  public void run() throws Exception {
    CustomActionContext context = getContext();
    Map<String, String> properties = context.getSpecification().getProperties();
    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID),
        BatchPhaseSpec.class);
    PipelinePhase phase = phaseSpec.getPhase();
    StageSpec stageSpec = phase.iterator().next();
    PluginContext pluginContext = new PipelinePluginContext(context, metrics,
        phaseSpec.isStageLoggingEnabled(),
        phaseSpec.isProcessTimingEnabled());
    PipelineRuntime pipelineRuntime = new PipelineRuntime(context, metrics);
    Action action =
        pluginContext.newPluginInstance(stageSpec.getName(),
            new DefaultMacroEvaluator(pipelineRuntime.getArguments(),
                context.getLogicalStartTime(),
                context, context,
                context.getNamespace()));
    ActionContext actionContext = new BasicActionContext(context, pipelineRuntime, stageSpec);
    if (!context.getDataTracer(stageSpec.getName()).isEnabled()) {
      action.run(actionContext);
    }
    WorkflowToken token = context.getWorkflowToken();
    if (token == null) {
      throw new IllegalStateException(
          "WorkflowToken cannot be null when action is executed through Workflow.");
    }
    for (Map.Entry<String, String> entry : pipelineRuntime.getArguments().getAddedArguments()
        .entrySet()) {
      token.put(entry.getKey(), entry.getValue());
    }
  }
}
