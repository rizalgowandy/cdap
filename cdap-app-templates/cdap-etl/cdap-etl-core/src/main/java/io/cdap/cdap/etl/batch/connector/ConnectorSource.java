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

package io.cdap.cdap.etl.batch.connector;

import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.workflow.WorkflowConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.common.Constants;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Internal batch source used as a connector between pipeline phases. Though this extends
 * BatchSource, this will not be instantiated through the plugin framework, but will be created
 * explicitly through the application.
 *
 * The batch connector is just a PartitionedFileSet, where a partition is the name of a phase that
 * wrote to it. This way, multiple phases can have the same local PartitionedFileSet as a sink, and
 * the source will read data from all partitions.
 *
 * This is because we don't want this to show up as a plugin that users can select and use, and also
 * because it uses features not exposed in the etl api (local workflow datasets).
 *
 * @param <T> type of output object
 */
public class ConnectorSource<T> extends BatchSource<LongWritable, Text, T> {

  // you can't read from the basedir of a FileSet so adding an arbitrary directory where data will be stored/read.
  static final String DATA_DIR = "data";
  private final String datasetName;

  protected ConnectorSource(String datasetName) {
    this.datasetName = datasetName;
  }

  // not the standard configurePipeline method. Need a workflowConfigurer to create a local dataset
  // we may want to expose local datasets in cdap-etl-api, but that is a separate track.
  public void configure(WorkflowConfigurer workflowConfigurer) {
    workflowConfigurer.createLocalDataset(datasetName, FileSet.class,
        FileSetProperties.builder()
            .setInputFormat(CombineTextInputFormat.class)
            .setInputProperty(FileInputFormat.INPUT_DIR_RECURSIVE, "true")
            .setOutputFormat(TextOutputFormat.class)
            .build());
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    Map<String, String> arguments = new HashMap<>();
    FileSetArguments.setInputPath(arguments, Constants.Connector.DATA_DIR);
    context.setInput(Input.ofDataset(datasetName, arguments));
  }

}
