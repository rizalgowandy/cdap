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

package io.cdap.cdap.app.mapreduce;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.metrics.MapReduceMetrics;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.MRJobInfo;
import io.cdap.cdap.proto.MRTaskInfo;
import io.cdap.cdap.proto.ProgramType;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapreduce.TaskCounter;

/**
 * Retrieves information/reports for a MapReduce run via the Metrics system.
 */
public class LocalMRJobInfoFetcher implements MRJobInfoFetcher {

  private final MetricsSystemClient metricsSystemClient;

  @Inject
  public LocalMRJobInfoFetcher(MetricsSystemClient metricsSystemClient) {
    this.metricsSystemClient = metricsSystemClient;
  }

  /**
   * @param runId for which information will be returned.
   * @return a {@link MRJobInfo} containing information about a particular MapReduce program run.
   */
  @Override
  public MRJobInfo getMRJobInfo(Id.Run runId) throws IOException {
    Preconditions.checkArgument(ProgramType.MAPREDUCE.equals(runId.getProgram().getType()));

    // baseTags has tag keys: ns.app.mr.runid
    Map<String, String> baseTags = Maps.newHashMap();
    baseTags.put(Constants.Metrics.Tag.NAMESPACE, runId.getNamespace().getId());
    baseTags.put(Constants.Metrics.Tag.APP, runId.getProgram().getApplicationId());
    baseTags.put(Constants.Metrics.Tag.MAPREDUCE, runId.getProgram().getId());
    baseTags.put(Constants.Metrics.Tag.RUN_ID, runId.getId());

    Map<String, String> mapTags = Maps.newHashMap(baseTags);
    mapTags.put(Constants.Metrics.Tag.MR_TASK_TYPE, MapReduceMetrics.TaskType.Mapper.getId());

    Map<String, String> reduceTags = Maps.newHashMap(baseTags);
    reduceTags.put(Constants.Metrics.Tag.MR_TASK_TYPE, MapReduceMetrics.TaskType.Reducer.getId());

    // map from RunId -> (CounterName -> CounterValue)
    Table<String, String, Long> mapTaskMetrics = HashBasedTable.create();
    Table<String, String, Long> reduceTaskMetrics = HashBasedTable.create();

    // Populate mapTaskMetrics and reduce Task Metrics via MetricStore. Used to construct MRTaskInfo below.
    Map<String, String> metricNamesToCounters = Maps.newHashMap();
    metricNamesToCounters.put(prependSystem(MapReduceMetrics.METRIC_TASK_INPUT_RECORDS),
        TaskCounter.MAP_INPUT_RECORDS.name());
    metricNamesToCounters.put(prependSystem(MapReduceMetrics.METRIC_TASK_OUTPUT_RECORDS),
        TaskCounter.MAP_OUTPUT_RECORDS.name());
    metricNamesToCounters.put(prependSystem(MapReduceMetrics.METRIC_TASK_BYTES),
        TaskCounter.MAP_OUTPUT_BYTES.name());
    metricNamesToCounters.put(prependSystem(MapReduceMetrics.METRIC_TASK_COMPLETION),
        MapReduceMetrics.METRIC_TASK_COMPLETION);

    // get metrics grouped by instance-id for the map tasks
    queryGroupedAggregates(mapTags, mapTaskMetrics, metricNamesToCounters);

    Map<String, Long> mapProgress = Maps.newHashMap();
    if (mapTaskMetrics.columnMap().containsKey(MapReduceMetrics.METRIC_TASK_COMPLETION)) {
      mapProgress = Maps.newHashMap(
          mapTaskMetrics.columnMap().remove(MapReduceMetrics.METRIC_TASK_COMPLETION));
    }

    Map<String, String> reduceMetricsToCounters = Maps.newHashMap();
    reduceMetricsToCounters.put(prependSystem(MapReduceMetrics.METRIC_TASK_INPUT_RECORDS),
        TaskCounter.REDUCE_INPUT_RECORDS.name());
    reduceMetricsToCounters.put(prependSystem(MapReduceMetrics.METRIC_TASK_OUTPUT_RECORDS),
        TaskCounter.REDUCE_OUTPUT_RECORDS.name());
    reduceMetricsToCounters.put(prependSystem(MapReduceMetrics.METRIC_TASK_COMPLETION),
        MapReduceMetrics.METRIC_TASK_COMPLETION);

    // get metrics grouped by instance-id for the map tasks
    queryGroupedAggregates(reduceTags, reduceTaskMetrics, reduceMetricsToCounters);

    Map<String, Long> reduceProgress = Maps.newHashMap();
    if (reduceTaskMetrics.columnMap().containsKey(MapReduceMetrics.METRIC_TASK_COMPLETION)) {
      reduceProgress = Maps.newHashMap(
          reduceTaskMetrics.columnMap().remove(MapReduceMetrics.METRIC_TASK_COMPLETION));
    }

    // Construct MRTaskInfos from the information we can get from Metric system.
    List<MRTaskInfo> mapTaskInfos = Lists.newArrayList();
    for (Map.Entry<String, Map<String, Long>> taskEntry : mapTaskMetrics.rowMap().entrySet()) {
      String mapTaskId = taskEntry.getKey();
      mapTaskInfos.add(new MRTaskInfo(mapTaskId, null, null, null,
          mapProgress.get(mapTaskId) / 100.0F, taskEntry.getValue()));
    }

    List<MRTaskInfo> reduceTaskInfos = Lists.newArrayList();
    for (Map.Entry<String, Map<String, Long>> taskEntry : reduceTaskMetrics.rowMap().entrySet()) {
      String reduceTaskId = taskEntry.getKey();
      reduceTaskInfos.add(new MRTaskInfo(reduceTaskId, null, null, null,
          reduceProgress.get(reduceTaskId) / 100.0F, taskEntry.getValue()));
    }

    return getJobCounters(mapTags, reduceTags, mapTaskInfos, reduceTaskInfos, runId);
  }


  private MRJobInfo getJobCounters(Map<String, String> mapTags, Map<String, String> reduceTags,
      List<MRTaskInfo> mapTaskInfos, List<MRTaskInfo> reduceTaskInfos,
      Id.Run runId) throws IOException {
    HashMap<String, Long> metrics = Maps.newHashMap();

    Map<String, String> mapMetricsToCounters =
        ImmutableMap.of(prependSystem(MapReduceMetrics.METRIC_INPUT_RECORDS),
            TaskCounter.MAP_INPUT_RECORDS.name(),
            prependSystem(MapReduceMetrics.METRIC_OUTPUT_RECORDS),
            TaskCounter.MAP_OUTPUT_RECORDS.name(),
            prependSystem(MapReduceMetrics.METRIC_BYTES), TaskCounter.MAP_OUTPUT_BYTES.name(),
            prependSystem(MapReduceMetrics.METRIC_COMPLETION), MapReduceMetrics.METRIC_COMPLETION);

    getAggregates(mapTags, mapMetricsToCounters, metrics);
    float mapProgress = metrics.remove(MapReduceMetrics.METRIC_COMPLETION) / 100.0F;

    Map<String, String> reduceMetricsToCounters =
        ImmutableMap.of(prependSystem(MapReduceMetrics.METRIC_INPUT_RECORDS),
            TaskCounter.REDUCE_INPUT_RECORDS.name(),
            prependSystem(MapReduceMetrics.METRIC_OUTPUT_RECORDS),
            TaskCounter.REDUCE_OUTPUT_RECORDS.name(),
            prependSystem(MapReduceMetrics.METRIC_COMPLETION), MapReduceMetrics.METRIC_COMPLETION);

    getAggregates(reduceTags, reduceMetricsToCounters, metrics);
    float reduceProgress = metrics.remove(MapReduceMetrics.METRIC_COMPLETION) / 100.0F;
    return new MRJobInfo(mapProgress, reduceProgress, metrics, mapTaskInfos, reduceTaskInfos,
        false);
  }

  private String prependSystem(String metric) {
    return "system." + metric;
  }

  private void getAggregates(Map<String, String> tags, Map<String, String> metricsToCounters,
      Map<String, Long> result) throws IOException {
    Collection<MetricTimeSeries> query = metricsSystemClient.query(tags,
        metricsToCounters.keySet());
    // initialize elements to zero
    for (String counterName : metricsToCounters.values()) {
      result.put(counterName, 0L);
    }
    for (MetricTimeSeries metricTimeSeries : query) {
      List<TimeValue> timeValues = metricTimeSeries.getTimeValues();
      TimeValue timeValue = Iterables.getOnlyElement(timeValues);
      result.put(metricsToCounters.get(metricTimeSeries.getMetricName()), timeValue.getValue());
    }
  }

  // queries MetricStore for one metric across all tasks of a certain TaskType, using GroupBy InstanceId
  private void queryGroupedAggregates(Map<String, String> tags,
      Table<String, String, Long> allTaskMetrics,
      Map<String, String> metricsToCounters) throws IOException {

    Collection<MetricTimeSeries> query = metricsSystemClient.query(
        tags, metricsToCounters.keySet(), Collections.singleton(Constants.Metrics.Tag.INSTANCE_ID));

    for (MetricTimeSeries metricTimeSeries : query) {
      List<TimeValue> timeValues = metricTimeSeries.getTimeValues();
      TimeValue timeValue = Iterables.getOnlyElement(timeValues);
      String taskId = metricTimeSeries.getTagValues().get(Constants.Metrics.Tag.INSTANCE_ID);
      allTaskMetrics.put(taskId, metricsToCounters.get(metricTimeSeries.getMetricName()),
          timeValue.getValue());
    }
  }
}
