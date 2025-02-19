/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Factory that creates a {@link DataframeCollection}
 */
public class DataframeCollectionFactory<T> implements BatchCollectionFactory<T> {

  private final Schema schema;
  private final Dataset<Row> dataFrame;

  /**
   * Creates dataFrame-based pull result
   *
   * @param schema schema of the dataFrame
   * @param dataFrame result dataFrame
   */
  public DataframeCollectionFactory(Schema schema, Dataset<Row> dataFrame) {
    this.schema = schema;
    this.dataFrame = dataFrame;
  }

  @Override
  public BatchCollection<T> create(JavaSparkExecutionContext sec, JavaSparkContext jsc,
      SQLContext sqlContext, DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory,
      FunctionCache.Factory functionCacheFactory) {
    return (BatchCollection<T>) new DataframeCollection(
        schema, dataFrame, sec, jsc, sqlContext, datasetContext,
        sinkFactory, functionCacheFactory);
  }
}
