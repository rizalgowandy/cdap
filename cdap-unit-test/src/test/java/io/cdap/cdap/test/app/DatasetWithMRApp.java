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

package io.cdap.cdap.test.app;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.metrics.Metrics;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * App which copies data from one KVTable to another using a MapReduce program.
 */
public class DatasetWithMRApp extends AbstractApplication {

  public static final String INPUT_KEY = "input";
  public static final String OUTPUT_KEY = "output";
  public static final String MAPREDUCE_PROGRAM = "copymr";

  @Override
  public void configure() {
    setDescription("Copy Data from one KVTable Dataset to another");
    addMapReduce(new CopyMapReduce());
  }

  public static class CopyMapReduce extends AbstractMapReduce {

    @Override
    public void configure() {
      setName(MAPREDUCE_PROGRAM);
    }

    @Override
    public void initialize() {
      MapReduceContext context = getContext();
      context.addInput(Input.ofDataset(context.getRuntimeArguments().get(INPUT_KEY)));
      context.addOutput(Output.ofDataset(context.getRuntimeArguments().get(OUTPUT_KEY)));
      Job hadoopJob = context.getHadoopJob();
      hadoopJob.setMapperClass(IdentityMapper.class);
      hadoopJob.setNumReduceTasks(0);
    }

    public static class IdentityMapper extends Mapper<byte[], byte[], byte[], byte[]> {
      private Metrics metrics;
      public void map(byte[] key, byte[] value, Context context) throws IOException, InterruptedException {
        metrics.gauge("test.metric", 10);
        context.write(key, value);
      }
    }
  }
}
