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

package io.cdap.cdap.internal.app.runtime.batch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.batch.dataset.output.ProvidedOutput;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for getting and setting specific config settings for a job context.
 */
public final class MapReduceContextConfig {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceContextConfig.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
          new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
      .create();
  private static final Type PLUGIN_MAP_TYPE = new TypeToken<Map<String, Plugin>>() {
  }.getType();
  private static final Type OUTPUT_LIST_TYPE = new TypeToken<List<Output.DatasetOutput>>() {
  }.getType();

  static final String HCONF_ATTR_PLUGINS = "cdap.mapreduce.plugins";
  private static final String HCONF_ATTR_APP_SPEC = "cdap.mapreduce.app.spec";
  private static final String HCONF_ATTR_PROGRAM_ID = "cdap.mapreduce.program.id";
  private static final String HCONF_ATTR_WORKFLOW_INFO = "cdap.mapreduce.workflow.info";
  private static final String HCONF_ATTR_PROGRAM_JAR_URI = "cdap.mapreduce.program.jar.uri";
  private static final String HCONF_ATTR_CCONF = "cdap.mapreduce.cconf";
  private static final String HCONF_ATTR_LOCAL_FILES = "cdap.mapreduce.local.files";
  private static final String HCONF_ATTR_PROGRAM_OPTIONS = "cdap.mapreduce.program.options";
  private static final String HCONF_ATTR_OUTPUTS = "cdap.mapreduce.outputs";

  private final Configuration hConf;

  public MapReduceContextConfig(Configuration hConf) {
    this.hConf = hConf;
  }

  Configuration getHConf() {
    return hConf;
  }

  /**
   * Updates the {@link Configuration} of this class with the given parameters.
   *
   * @param context the context for the MapReduce program
   * @param conf the CDAP configuration
   * @param programJarURI The URI of the program JAR
   * @param localizedUserResources the localized resources for the MapReduce program
   */
  public void set(BasicMapReduceContext context, CConfiguration conf, URI programJarURI,
      Map<String, String> localizedUserResources) {
    setProgramOptions(context.getProgramOptions());
    setProgramId(context.getProgram().getId());
    setApplicationSpecification(context.getApplicationSpecification());
    setWorkflowProgramInfo(context.getWorkflowInfo());
    setPlugins(context.getApplicationSpecification().getPlugins());
    setProgramJarURI(programJarURI);
    setConf(conf);
    setLocalizedResources(localizedUserResources);
    setOutputs(context.getOutputs());
  }

  private void setOutputs(List<ProvidedOutput> providedOutputs) {
    // we only need to serialize the original Output objects, not the entire ProvidedOutput
    List<Output.DatasetOutput> datasetOutputs = new ArrayList<>();
    for (ProvidedOutput providedOutput : providedOutputs) {
      Output output = providedOutput.getOutput();
      if (output instanceof Output.DatasetOutput) {
        datasetOutputs.add((Output.DatasetOutput) output);
      }
    }
    hConf.set(HCONF_ATTR_OUTPUTS, GSON.toJson(datasetOutputs));
  }

  /**
   * @return the list of DatasetOutputs configured for this MapReduce job.
   */
  public List<Output.DatasetOutput> getOutputs() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_OUTPUTS), OUTPUT_LIST_TYPE);
  }

  private void setProgramId(ProgramId programId) {
    hConf.set(HCONF_ATTR_PROGRAM_ID, GSON.toJson(programId));
  }

  /**
   * Serialize the {@link ApplicationSpecification} to the configuration.
   */
  @VisibleForTesting
  void setApplicationSpecification(ApplicationSpecification spec) {
    hConf.set(HCONF_ATTR_APP_SPEC, GSON.toJson(spec, ApplicationSpecification.class));
  }

  /**
   * Returns the {@link ProgramId} for the MapReduce program.
   */
  public ProgramId getProgramId() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_PROGRAM_ID), ProgramId.class);
  }

  /**
   * @return the {@link ApplicationSpecification} stored in the configuration.
   */
  public ApplicationSpecification getApplicationSpecification() {
    return GSON.fromJson(hConf.getRaw(HCONF_ATTR_APP_SPEC), ApplicationSpecification.class);
  }

  private void setWorkflowProgramInfo(@Nullable WorkflowProgramInfo info) {
    if (info != null) {
      hConf.set(HCONF_ATTR_WORKFLOW_INFO, GSON.toJson(info));
    }
  }

  /**
   * Returns the {@link WorkflowProgramInfo} if it is running inside Workflow or {@code null} if
   * not.
   */
  @Nullable
  WorkflowProgramInfo getWorkflowProgramInfo() {
    String info = hConf.get(HCONF_ATTR_WORKFLOW_INFO);
    if (info == null) {
      return null;
    }
    WorkflowProgramInfo workflowProgramInfo = GSON.fromJson(info, WorkflowProgramInfo.class);
    workflowProgramInfo.getWorkflowToken().disablePut();
    return workflowProgramInfo;
  }

  private void setPlugins(Map<String, Plugin> plugins) {
    hConf.set(HCONF_ATTR_PLUGINS, GSON.toJson(plugins, PLUGIN_MAP_TYPE));
  }

  /**
   * Returns the plugins being used in the MapReduce program.
   */
  public Map<String, Plugin> getPlugins() {
    String spec = hConf.getRaw(HCONF_ATTR_PLUGINS);
    if (spec == null) {
      return ImmutableMap.of();
    }
    return GSON.fromJson(spec, PLUGIN_MAP_TYPE);
  }

  private void setProgramJarURI(URI programJarURI) {
    hConf.set(HCONF_ATTR_PROGRAM_JAR_URI, programJarURI.toASCIIString());
  }

  /**
   * Returns the URI of where the program JAR is.
   */
  URI getProgramJarURI() {
    return URI.create(hConf.get(HCONF_ATTR_PROGRAM_JAR_URI));
  }

  /**
   * Returns the file name of the program JAR.
   */
  String getProgramJarName() {
    return new Path(getProgramJarURI()).getName();
  }

  private void setLocalizedResources(Map<String, String> localizedUserResources) {
    hConf.set(HCONF_ATTR_LOCAL_FILES, GSON.toJson(localizedUserResources));
  }

  Map<String, File> getLocalizedResources() {
    Map<String, String> nameToPath = GSON.fromJson(hConf.get(HCONF_ATTR_LOCAL_FILES),
        new TypeToken<Map<String, String>>() {
        }.getType());
    Map<String, File> nameToFile = new HashMap<>();
    for (Map.Entry<String, String> entry : nameToPath.entrySet()) {
      nameToFile.put(entry.getKey(), new File(entry.getValue()));
    }
    return nameToFile;
  }

  private void setProgramOptions(ProgramOptions programOptions) {
    hConf.set(HCONF_ATTR_PROGRAM_OPTIONS, GSON.toJson(programOptions, ProgramOptions.class));
  }

  /**
   * Returns the {@link ProgramOptions} from the configuration.
   */
  public ProgramOptions getProgramOptions() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_PROGRAM_OPTIONS), ProgramOptions.class);
  }

  private void setConf(CConfiguration conf) {
    StringWriter stringWriter = new StringWriter();
    try {
      conf.writeXml(stringWriter);
    } catch (IOException e) {
      LOG.error("Unable to serialize CConfiguration into xml");
      throw Throwables.propagate(e);
    }
    hConf.set(HCONF_ATTR_CCONF, stringWriter.toString());
  }

  /**
   * Returns the {@link CConfiguration} stored inside the job {@link Configuration}.
   */
  CConfiguration getCConf() {
    String conf = hConf.getRaw(HCONF_ATTR_CCONF);
    Preconditions.checkArgument(conf != null, "No CConfiguration available");
    return CConfiguration.create(new ByteArrayInputStream(conf.getBytes(Charsets.UTF_8)));
  }
}
