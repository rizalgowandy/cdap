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

package io.cdap.cdap.etl.spark.function;

import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;

import javax.annotation.Nullable;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Function that doesn't transform anything, but just emits counts for the number of records from that stage.
 *
 * @param <T> the type of input object
 */
public class CountingFunction<T> implements Function<T, T>, MapFunction<T, T> {
  private final String stageName;
  private final Metrics metrics;
  private final String metricName;
  private final DataTracer dataTracer;
  private transient StageMetrics stageMetrics;

  // DataTracer is null for records.in
  public CountingFunction(String stageName, Metrics metrics, String metricName, @Nullable DataTracer dataTracer) {
    this.stageName = stageName;
    this.metrics = metrics;
    this.metricName = metricName;
    this.dataTracer = dataTracer;
  }

  @Override
  public T call(T in) throws Exception {
    if (stageMetrics == null) {
      stageMetrics = new DefaultStageMetrics(metrics, stageName);
    }
    // we only want to trace the data for records.out
    if (dataTracer != null && dataTracer.isEnabled()) {
      dataTracer.info(metricName, in);
    }
    stageMetrics.count(metricName, 1);
    return in;
  }

  private boolean filter(T in) throws Exception {
    call(in);
    return true;
  }

  /**
   * Helper method to represent this as {@link FilterFunction}. It's easier for Spark DataSet API
   * as mapping requires to provides a result encoder, while filter automatically reuses original
   * encoder.
   *
   * @return this function as {@link FilterFunction}.
   */
  public FilterFunction<T> asFilter() {
    return this::filter;
  }
}
