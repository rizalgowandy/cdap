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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.mapreduce.MRJobInfoFetcher;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.TimeMathParser;
import io.cdap.cdap.internal.app.store.WorkflowTable;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.WorkflowStatistics;
import io.cdap.cdap.proto.WorkflowStatsComparison;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Workflow Statistics Handler
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class WorkflowStatsSLAHttpHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private final Store store;
  private final MRJobInfoFetcher mrJobInfoFetcher;
  private final MetricsSystemClient metricsSystemClient;

  @Inject
  WorkflowStatsSLAHttpHandler(Store store, MRJobInfoFetcher mrJobInfoFetcher,
      MetricsSystemClient metricsSystemClient) {
    this.store = store;
    this.mrJobInfoFetcher = mrJobInfoFetcher;
    this.metricsSystemClient = metricsSystemClient;
  }

  /**
   * Returns the statistics for a given workflow.
   *
   * @param request The request
   * @param responder The responder
   * @param namespaceId The namespace the application is in
   * @param appId The application the workflow is in
   * @param workflowId The workflow that needs to have it stats shown
   * @param start The start time of the range
   * @param end The end time of the range
   * @param percentiles The list of percentile values on which visibility is needed
   */
  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/statistics")
  public void workflowStats(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("workflow-id") String workflowId,
      @QueryParam("start") @DefaultValue("now-1d") String start,
      @QueryParam("end") @DefaultValue("now") String end,
      @QueryParam("percentile") @DefaultValue("90.0") List<Double> percentiles) throws Exception {
    long startTime = TimeMathParser.parseTimeInSeconds(start);
    long endTime = TimeMathParser.parseTimeInSeconds(end);

    if (startTime < 0) {
      throw new BadRequestException("Invalid start time. The time you entered was : " + startTime);
    } else if (endTime < 0) {
      throw new BadRequestException("Invalid end time. The time you entered was : " + endTime);
    } else if (endTime < startTime) {
      throw new BadRequestException(
          "Start time : " + startTime + " cannot be larger than end time : " + endTime);
    }

    for (double i : percentiles) {
      if (i < 0.0 || i > 100.0) {
        throw new BadRequestException(
            "Percentile values have to be greater than or equal to 0 and"
                + " less than or equal to 100. Invalid input was " + Double.toString(i));
      }
    }
    WorkflowStatistics workflowStatistics = store.getWorkflowStatistics(
        new NamespaceId(namespaceId), appId, workflowId,
        startTime, endTime, percentiles);

    if (workflowStatistics == null) {
      responder.sendString(HttpResponseStatus.OK,
          "There are no statistics associated with this workflow : "
              + workflowId + " in the specified time range.");
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(workflowStatistics));
  }

  /**
   * The endpoint returns a list of workflow metrics based on the workflow run and a surrounding
   * number of runs of the workflow that are spaced apart by a time interval from each other.
   *
   * @param request The request
   * @param responder The responder
   * @param namespaceId The namespace the application is in
   * @param appId The application the workflow is in
   * @param workflowId The workflow that needs to have it stats shown
   * @param runId The run id of the Workflow that the user wants to see
   * @param limit The number of the records that the user wants to compare against on either
   *     side of the run
   * @param interval The timeInterval with which the user wants to space out the runs
   */
  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/statistics")
  public void workflowRunDetail(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("workflow-id") String workflowId,
      @PathParam("run-id") String runId,
      @QueryParam("limit") @DefaultValue("10") int limit,
      @QueryParam("interval") @DefaultValue("10s") String interval) throws Exception {
    if (limit <= 0) {
      throw new BadRequestException("Limit has to be greater than 0. Entered value was : " + limit);
    }

    long timeInterval;
    try {
      timeInterval = TimeMathParser.resolutionInSeconds(interval);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(
          "Interval is specified with invalid time unit. It should be specified with one"
              + " of the 'ms', 's', 'm', 'h', 'd' units. Entered value was : " + interval);
    }

    if (timeInterval <= 0) {
      throw new BadRequestException(
          "Interval should be greater than 0 and should be specified with one of the 'ms',"
              + " 's', 'm', 'h', 'd' units. Entered value was : " + interval);
    }
    Collection<WorkflowTable.WorkflowRunRecord> workflowRunRecords =
        store.retrieveSpacedRecords(new NamespaceId(namespaceId), appId, workflowId, runId, limit,
            timeInterval);

    List<WorkflowRunMetrics> workflowRunMetricsList = new ArrayList<>();
    Map<String, Long> startTimes = new HashMap<>();
    for (WorkflowTable.WorkflowRunRecord workflowRunRecord : workflowRunRecords) {
      workflowRunMetricsList.add(getDetailedRecord(new NamespaceId(namespaceId), appId, workflowId,
          workflowRunRecord.getWorkflowRunId()));
      startTimes.put(workflowRunRecord.getWorkflowRunId(),
          RunIds.getTime(RunIds.fromString(workflowRunRecord.getWorkflowRunId()),
              TimeUnit.SECONDS));
    }

    Collection<WorkflowStatsComparison.ProgramNodes> formattedStatisticsMap = format(
        workflowRunMetricsList);
    responder.sendJson(HttpResponseStatus.OK,
        GSON.toJson(new WorkflowStatsComparison(startTimes, formattedStatisticsMap)));
  }

  /**
   * Compare the metrics of 2 runs of a workflow
   *
   * @param request The request
   * @param responder The responder
   * @param namespaceId The namespace the application is in
   * @param appId The application the workflow is in
   * @param workflowId The workflow that needs to have it stats shown
   * @param runId The run id of the Workflow that the user wants to see
   * @param otherRunId The other run id of the same workflow that the user wants to compare
   *     against
   */
  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/compare")
  public void compare(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("workflow-id") String workflowId,
      @PathParam("run-id") String runId,
      @QueryParam("other-run-id") String otherRunId) throws Exception {
    WorkflowRunMetrics detailedStatistics = getDetailedRecord(new NamespaceId(namespaceId), appId,
        workflowId, runId);
    WorkflowRunMetrics otherDetailedStatistics = getDetailedRecord(new NamespaceId(namespaceId),
        appId, workflowId,
        otherRunId);
    if (detailedStatistics == null) {
      throw new NotFoundException("The run-id provided was not found : " + runId);
    }
    if (otherDetailedStatistics == null) {
      throw new NotFoundException("The other run-id provided was not found : " + otherRunId);
    }

    List<WorkflowRunMetrics> workflowRunMetricsList = new ArrayList<>();
    workflowRunMetricsList.add(detailedStatistics);
    workflowRunMetricsList.add(otherDetailedStatistics);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(format(workflowRunMetricsList)));
  }

  private Collection<WorkflowStatsComparison.ProgramNodes> format(
      List<WorkflowRunMetrics> workflowRunMetricsList) {
    Map<String, WorkflowStatsComparison.ProgramNodes> programLevelDetails = new HashMap<>();
    for (WorkflowRunMetrics workflowRunMetrics : workflowRunMetricsList) {
      for (ProgramMetrics programMetrics : workflowRunMetrics.getProgramMetricsList()) {
        String programName = programMetrics.getProgramName();
        if (programLevelDetails.get(programName) == null) {
          WorkflowStatsComparison.ProgramNodes programNodes = new WorkflowStatsComparison.ProgramNodes(
              programName, programMetrics.getProgramType(),
              new ArrayList<>());
          programLevelDetails.put(programName, programNodes);
        }
        programLevelDetails.get(programName).addWorkflowDetails(
            workflowRunMetrics.getWorkflowRunId(), programMetrics.getProgramRunId(),
            programMetrics.getProgramStartTime(), programMetrics.getMetrics());
      }
    }
    return programLevelDetails.values();
  }

  /**
   * Returns the detailed Record for the Workflow
   *
   * @param namespaceId The namespace that the workflow belongs to
   * @param appName The application that the workflow belongs to
   * @param workflowName The Workflow that needs to get its detailed record
   * @param runId Run Id of the workflow
   * @return Return the Workflow Run Metrics
   */
  @Nullable
  private WorkflowRunMetrics getDetailedRecord(NamespaceId namespaceId, String appName,
      String workflowName,
      String runId)
      throws Exception {
    WorkflowTable.WorkflowRunRecord workflowRunRecord = store.getWorkflowRun(namespaceId, appName,
        workflowName, runId);
    if (workflowRunRecord == null) {
      return null;
    }
    List<WorkflowTable.ProgramRun> programRuns = workflowRunRecord.getProgramRuns();
    List<ProgramMetrics> programMetricsList = new ArrayList<>();
    for (WorkflowTable.ProgramRun programRun : programRuns) {
      Map<String, Long> programMap = new HashMap<>();
      String programName = programRun.getName();
      ProgramType programType = programRun.getProgramType();
      ProgramId program = new ProgramId(namespaceId.getNamespace(), appName, programType,
          programName);
      String programRunId = programRun.getRunId();
      if (programType == ProgramType.MAPREDUCE) {
        programMap = getMapreduceDetails(program, programRunId);
      } else if (programType == ProgramType.SPARK) {
        programMap = getSparkDetails(program, programRunId);
      }
      programMap.put("timeTaken", programRun.getTimeTaken());
      long programStartTime = RunIds.getTime(RunIds.fromString(programRunId), TimeUnit.SECONDS);
      programMetricsList.add(
          new ProgramMetrics(programName, programType, programRunId, programStartTime, programMap));
    }
    return new WorkflowRunMetrics(runId, programMetricsList);
  }

  private Map<String, Long> getMapreduceDetails(ProgramId mapreduceProgram, String runId)
      throws Exception {
    return mrJobInfoFetcher.getMRJobInfo(Id.Run.fromEntityId(mapreduceProgram.run(runId)))
        .getCounters();
  }

  private Map<String, Long> getSparkDetails(ProgramId sparkProgram, String runId)
      throws IOException {
    Map<String, String> context = new HashMap<>();
    context.put(Constants.Metrics.Tag.NAMESPACE, sparkProgram.getNamespace());
    context.put(Constants.Metrics.Tag.APP, sparkProgram.getApplication());
    context.put(Constants.Metrics.Tag.SPARK, sparkProgram.getProgram());
    context.put(Constants.Metrics.Tag.RUN_ID, runId);

    Collection<String> metricNames = metricsSystemClient.search(context);
    Collection<MetricTimeSeries> queryResult = metricsSystemClient.query(context, metricNames);

    Map<String, Long> overallResult = new HashMap<>();
    for (MetricTimeSeries timeSeries : queryResult) {
      overallResult.put(timeSeries.getMetricName(), timeSeries.getTimeValues().get(0).getValue());
    }

    return overallResult;
  }

  private static class ProgramMetrics {

    private final String programName;
    private final ProgramType programType;
    private final String programRunId;
    private final long programStartTime;
    private final Map<String, Long> metrics;

    private ProgramMetrics(String programName, ProgramType programType, String programRunId,
        long programStartTime, Map<String, Long> metrics) {
      this.programName = programName;
      this.programType = programType;
      this.programRunId = programRunId;
      this.programStartTime = programStartTime;
      this.metrics = metrics;
    }

    String getProgramName() {
      return programName;
    }

    ProgramType getProgramType() {
      return programType;
    }

    Map<String, Long> getMetrics() {
      return metrics;
    }

    long getProgramStartTime() {
      return programStartTime;
    }

    String getProgramRunId() {
      return programRunId;
    }
  }

  private static class WorkflowRunMetrics {

    private final String workflowRunId;
    private final List<ProgramMetrics> programMetricsList;

    private WorkflowRunMetrics(String workflowRunId, List<ProgramMetrics> programMetricsList) {
      this.workflowRunId = workflowRunId;
      this.programMetricsList = programMetricsList;
    }

    String getWorkflowRunId() {
      return workflowRunId;
    }

    List<ProgramMetrics> getProgramMetricsList() {
      return programMetricsList;
    }
  }
}
