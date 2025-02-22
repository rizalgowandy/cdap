/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.sql.engine.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;


/**
 * Implementation for SparkRecordCollection.
 */
public class SparkRecordCollectionImpl implements SparkRecordCollection {

  private final Dataset<Row> dataFrame;

  public SparkRecordCollectionImpl(Dataset<Row> dataset) {
    this.dataFrame = dataset;
  }

  @Override
  public Dataset<Row> getDataFrame() {
    return dataFrame;
  }
}
