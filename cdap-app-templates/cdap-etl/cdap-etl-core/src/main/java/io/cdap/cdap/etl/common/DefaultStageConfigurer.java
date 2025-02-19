/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.MultiInputStageConfigurer;
import io.cdap.cdap.etl.api.MultiOutputStageConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.validation.DefaultFailureCollector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * This stores the input schemas that is passed to this stage from other stages in the pipeline and
 * the output schema that could be sent to the next stages from this stage. Currently we only allow
 * multiple input/output schema per stage except for {@link io.cdap.cdap.etl.api.Joiner} where we
 * allow multiple input schemas
 */
public class DefaultStageConfigurer implements StageConfigurer, MultiInputStageConfigurer,
    MultiOutputStageConfigurer {

  private String stageName;
  private Schema outputSchema;
  private Schema outputErrorSchema;
  private boolean errorSchemaSet;
  private FailureCollector collector;
  protected Map<String, Schema> inputSchemas;
  protected List<String> inputStages;
  protected Map<String, Schema> outputPortSchemas;

  public DefaultStageConfigurer(String stageName) {
    this.inputSchemas = new HashMap<>();
    this.inputStages = new ArrayList<>();
    this.outputPortSchemas = new HashMap<>();
    this.errorSchemaSet = false;
    this.stageName = stageName;
  }

  @Nullable
  public Schema getOutputSchema() {
    return outputSchema;
  }

  public Map<String, Schema> getOutputPortSchemas() {
    return outputPortSchemas;
  }

  @Override
  @Nullable
  public Schema getInputSchema() {
    return inputSchemas.isEmpty() ? null : inputSchemas.entrySet().iterator().next().getValue();
  }

  @Override
  public void setOutputSchemas(Map<String, Schema> outputSchemas) {
    outputPortSchemas.putAll(outputSchemas);
  }

  @Override
  public Map<String, Schema> getInputSchemas() {
    return inputSchemas;
  }

  @Override
  public List<String> getInputStages() {
    return inputStages;
  }

  @Override
  public void setOutputSchema(@Nullable Schema outputSchema) {
    this.outputSchema = outputSchema;
  }

  @Override
  public void setErrorSchema(@Nullable Schema errorSchema) {
    this.outputErrorSchema = errorSchema;
    errorSchemaSet = true;
  }

  @Override
  public String getStageName() {
    return stageName;
  }

  @Override
  public FailureCollector getFailureCollector() {
    if (collector == null) {
      this.collector = new DefaultFailureCollector(stageName, inputSchemas);
    }
    return collector;
  }

  public Schema getErrorSchema() {
    if (errorSchemaSet) {
      return outputErrorSchema;
    }
    // only joiners can have multiple input schemas, and joiners can't emit errors
    return inputSchemas.isEmpty() ? null : inputSchemas.values().iterator().next();
  }

  public void addInputSchema(String inputStageName, @Nullable Schema inputSchema) {
    inputSchemas.put(inputStageName, inputSchema);
  }

  public void addInputStage(String inputStageName) {
    inputStages.add(inputStageName);
  }
}

