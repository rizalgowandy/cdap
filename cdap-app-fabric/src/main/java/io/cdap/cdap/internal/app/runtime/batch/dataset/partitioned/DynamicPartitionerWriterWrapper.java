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

package io.cdap.cdap.internal.app.runtime.batch.dataset.partitioned;

import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.dataset.DataSetException;
import io.cdap.cdap.api.dataset.lib.DynamicPartitioner;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetArguments;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import io.cdap.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDataset;
import io.cdap.cdap.internal.app.runtime.batch.BasicMapReduceTaskContext;
import io.cdap.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A RecordWriter that allows writing dynamically to multiple partitions of a PartitionedFileSet.
 */
abstract class DynamicPartitionerWriterWrapper<K, V> extends RecordWriter<K, V> {

  private final PartitionedFileSet outputDataset;
  private final DynamicPartitioner.PartitionWriteOption partitionWriteOption;
  private final TaskAttemptContext job;
  private final String outputName;
  private final Partitioning partitioning;
  private final String fileOutputFormatName;

  @SuppressWarnings("unchecked")
  final DynamicPartitioner<K, V> dynamicPartitioner;
  final BasicMapReduceTaskContext<K, V> taskContext;

  DynamicPartitionerWriterWrapper(TaskAttemptContext job) {
    this.job = job;

    Configuration configuration = job.getConfiguration();
    Class<? extends DynamicPartitioner> partitionerClass = configuration
        .getClass(PartitionedFileSetArguments.DYNAMIC_PARTITIONER_CLASS_NAME, null,
            DynamicPartitioner.class);
    this.dynamicPartitioner = new InstantiatorFactory(false).get(TypeToken.of(partitionerClass))
        .create();
    this.partitionWriteOption =
        DynamicPartitioner.PartitionWriteOption.valueOf(
            configuration.get(PartitionedFileSetArguments.DYNAMIC_PARTITIONER_WRITE_OPTION));

    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(configuration);
    this.taskContext = classLoader.getTaskContextProvider().get(job);

    // name the output file 'part-<RunId>-m-00000' instead of 'part-m-00000'
    String outputName = DynamicPartitioningOutputFormat.getOutputName(job);
    if (partitionWriteOption == DynamicPartitioner.PartitionWriteOption.CREATE_OR_APPEND) {
      outputName = outputName + "-" + taskContext.getProgramRunId().getRun();
    }
    this.outputName = outputName;

    String outputDatasetName = configuration.get(
        Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_DATASET);
    this.outputDataset = taskContext.getDataset(outputDatasetName);
    this.partitioning = outputDataset.getPartitioning();

    this.dynamicPartitioner.initialize(taskContext);
    this.fileOutputFormatName = job.getConfiguration()
        .getClass(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_FORMAT_CLASS_NAME, null,
            FileOutputFormat.class)
        .getName();
  }

  // returns a TaskAttemptContext whose configuration will reflect the specified partitionKey's path as the output path
  TaskAttemptContext getKeySpecificContext(PartitionKey partitionKey) throws IOException {
    if (partitionWriteOption == DynamicPartitioner.PartitionWriteOption.CREATE) {
      if (outputDataset.getPartition(partitionKey) != null) {
        // TODO: throw PartitionAlreadyExists exception? (include dataset name also?)
        throw new DataSetException("Partition already exists: " + partitionKey);
      }
    }
    String relativePath = PartitionedFileSetDataset.getOutputPath(partitionKey, partitioning);
    String finalPath = relativePath + "/" + outputName;
    return getTaskAttemptContext(job, finalPath);
  }

  /**
   * @return A RecordWriter object for the given TaskAttemptContext (configured for a particular
   *     file name).
   */
  RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    // Get a new FileOutputFormat object when creating a RecordWriter, so that different RecordWriter's can have
    // different underlying objects such as WriteSupport. This is a fix for CDAP-12823.
    return createFileOutputFormat().getRecordWriter(job);
  }

  // returns a TaskAttemptContext whose configuration will reflect the specified newOutputName as the output path
  private TaskAttemptContext getTaskAttemptContext(TaskAttemptContext context,
      String newOutputName) throws IOException {
    Job job = new Job(context.getConfiguration());
    DynamicPartitioningOutputFormat.setOutputName(job, newOutputName);
    // CDAP-4806 We must set this parameter in addition to calling FileOutputFormat#setOutputName, because
    // AvroKeyOutputFormat/AvroKeyValueOutputFormat use a different parameter for the output name than FileOutputFormat.
    if (isAvroOutputFormat(fileOutputFormatName)) {
      job.getConfiguration().set("avro.mo.config.namedOutput", newOutputName);
    }

    Path jobOutputPath = DynamicPartitioningOutputFormat.createJobSpecificPath(
        FileOutputFormat.getOutputPath(job),
        context);
    DynamicPartitioningOutputFormat.setOutputPath(job, jobOutputPath);

    return new TaskAttemptContextImpl(job.getConfiguration(), context.getTaskAttemptID());
  }

  private FileOutputFormat<K, V> createFileOutputFormat() {
    Class<? extends FileOutputFormat> delegateOutputFormat = job.getConfiguration()
        .getClass(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_FORMAT_CLASS_NAME, null,
            FileOutputFormat.class);

    @SuppressWarnings("unchecked")
    FileOutputFormat<K, V> fileOutputFormat =
        ReflectionUtils.newInstance(delegateOutputFormat, job.getConfiguration());
    return fileOutputFormat;
  }

  private static boolean isAvroOutputFormat(String fileOutputFormatName) {
    // use class name String in order avoid having a dependency on the Avro libraries here
    return "org.apache.avro.mapreduce.AvroKeyOutputFormat".equals(fileOutputFormatName)
        || "org.apache.avro.mapreduce.AvroKeyValueOutputFormat".equals(fileOutputFormatName);
  }
}
