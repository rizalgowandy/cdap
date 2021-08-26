/*
 * Copyright © 2015-2020 Cask Data, Inc.
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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.logging.LoggingConfiguration;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.filter.FilterParser;
import io.cdap.cdap.logging.gateway.handlers.AbstractChunkedLogProducer;
import io.cdap.cdap.logging.gateway.handlers.TextChunkedLogProducer;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.metrics.query.MetricsQueryHelper;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.MetricQueryResult;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.WorkflowTokenNodeDetail;
import io.cdap.cdap.proto.codec.WorkflowTokenDetailCodec;
import io.cdap.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hadoop.hbase.util.Pair;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/** Support Bundle HTTP Handler. */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class SupportBundleHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON =
      new GsonBuilder()
          .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
          .registerTypeAdapter(WorkflowTokenNodeDetail.class, new WorkflowTokenNodeDetailCodec())
          .registerTypeAdapter(Trigger.class, new TriggerCodec())
          .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
          .registerTypeAdapter(Constraint.class, new ConstraintCodec())
          .create();

  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final Store store;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final MetricsQueryHelper metricsQueryHelper;
  private final LogReader logReader;
  private final String logPattern;

  @Inject
  SupportBundleHttpHandler(
      Store store,
      NamespaceQueryAdmin namespaceQueryAdmin,
      ApplicationLifecycleService applicationLifecycleService,
      MetricsQueryHelper metricsQueryHelper,
      LogReader logReader,
      CConfiguration cConfig) {
    this.store = store;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.applicationLifecycleService = applicationLifecycleService;
    this.metricsQueryHelper = metricsQueryHelper;
    this.logReader = logReader;
    this.logPattern =
        cConfig.get(LoggingConfiguration.LOG_PATTERN, LoggingConfiguration.DEFAULT_LOG_PATTERN);
  }

  /** Waits for each thread to end */
  public static void joinThread(Thread thread) {
    try {
      thread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Generate the Support Bundle if valid application id, workflow id, and runid are provided.
   *
   * @param namespaceId the namespace id
   * @param appId the app id
   * @param workflowName the workflow name
   * @param runId the runid of the workflow
   * @param needPipelineLog whether the customer need pipeline log file or not
   * @param needSystemLog whether the customer need system log(appfabric) file or not return the
   *     uuid of this support bundle
   * @throws NotFoundException is thrown when the application, workflow, or runid is not found
   */
  @POST
  @Path("/support/bundle")
  public void createSupportBundle(
      HttpRequest request,
      HttpResponder responder,
      @Nullable @QueryParam("namespace-id") String namespaceId,
      @Nullable @QueryParam("app-id") String appId,
      @Nullable @QueryParam("workflow-name") String workflowName,
      @Nullable @QueryParam("run-id") String runId,
      @DefaultValue("false") @QueryParam("need-pipeline-log") Boolean needPipelineLog,
      @DefaultValue("false") @QueryParam("need-system-log") Boolean needSystemLog)
      throws Exception {
    UUID uuid =
        generateSupportBundle(
            namespaceId, appId, workflowName, runId, needPipelineLog, needSystemLog);
    responder.sendString(
        HttpResponseStatus.OK, String.format("Support Bundle %s generated.", uuid));
  }

  /** Generates Support Bundle for the given parameters. */
  private UUID generateSupportBundle(
      String namespaceId,
      String appId,
      String workflowName,
      String runId,
      Boolean needPipelineLog,
      Boolean needSystemLog)
      throws Exception {
    // Generate a universal unique id for each bundle
    if (namespaceId != null) {
      NamespaceId namespace = new NamespaceId(namespaceId);
      if (!namespaceQueryAdmin.exists(namespace)) {
        throw new NamespaceNotFoundException(namespace);
      }
    }
    UUID uuid = UUID.randomUUID();
    ProgramRunId latestProgramRunId = null;
    RunRecordDetail latestRunRecordDetail = null;
    if (workflowName == null) {
      workflowName = "DataPipelineWorkflow";
    }
    List<Thread> threadList = new ArrayList<>();
    List<String> namespaceList = new ArrayList<>();
    // Puts all the files under the uuid path
    String homePath = System.getProperty("user.home") + "/support/bundle";
    File baseDirectory = new File(homePath);
    int fileCount = 1;
    if (baseDirectory.list() != null && baseDirectory.list().length > 0) {
      fileCount = baseDirectory.list().length;
    }

    // We want to keep consistent number of bundle to provide to customer
    if (fileCount >= 7) {
      File oldFilesDirectory = getOldestFolder(baseDirectory);
      deleteOldFolders(oldFilesDirectory);
    }
    File basePath = new File(homePath, uuid.toString());
    if (!basePath.exists()) {
      basePath.mkdirs();
    }
    // Generates statusJson to keep updates the status
    FileWriter statusJson = null;
    if (namespaceId == null) {
      namespaceList =
          namespaceQueryAdmin.list().stream()
              .map(meta -> meta.getName())
              .collect(Collectors.toList());
    } else {
      namespaceList.add(namespaceId);
    }
    for (String namespacesId : namespaceList) {
      NamespaceId namespace = new NamespaceId(namespacesId);
      Set<String> artifactNames = new HashSet<>();
      artifactNames.add("cdap-data-pipeline");
      List<ApplicationRecord> apps = new ArrayList<>();
      if (appId == null) {
        apps =
            applicationLifecycleService.getApps(namespace, artifactNames, null).stream()
                .map(ApplicationRecord::new)
                .collect(Collectors.toList());
      } else {
        apps.add(
            new ApplicationRecord(
                applicationLifecycleService.getAppDetail(new ApplicationId(namespaceId, appId))));
      }
      if (runId == null) {
        for (ApplicationRecord app : apps) {
          latestProgramRunId = null;
          latestRunRecordDetail = null;
          File appFolderPath = new File(basePath.toString(), app.getName());
          if (!appFolderPath.exists()) {
            appFolderPath.mkdirs();
          }
          statusJson = new FileWriter(appFolderPath + "/" + "status.json");
          // Generates application file and get application id and detail
          Pair<ApplicationId, ApplicationDetail> applicationPair =
              generateApplicationFile(
                  namespaceId, app.getName(), appFolderPath.toString(), threadList, statusJson);
          Map<ProgramRunId, RunRecordDetail> runMap =
              store.getRuns(
                  applicationPair.getFirst(),
                  ProgramRunStatus.ALL,
                  Integer.MAX_VALUE,
                  meta -> true);
          long startTs = 0;
          // Gets the latest run info
          for (ProgramRunId programRunId : runMap.keySet()) {
            if (startTs < runMap.get(programRunId).getStartTs()
                && programRunId.getProgram().equals("DataPipelineWorkflow")) {
              latestProgramRunId = programRunId;
              latestRunRecordDetail = runMap.get(programRunId);
              startTs = runMap.get(programRunId).getStartTs();
            }
          }
          if (latestProgramRunId != null) {
            JsonObject metrics =
                queryMetrics(
                    namespaceId,
                    app.getName(),
                    latestProgramRunId.getRun(),
                    workflowName,
                    applicationPair.getSecond().getConfiguration(),
                    latestRunRecordDetail != null ? latestRunRecordDetail.getStartTs() : 0,
                    latestRunRecordDetail != null && latestRunRecordDetail.getStopTs() != null
                        ? latestRunRecordDetail.getStopTs()
                        : DateTime.now().getMillis());
            generateLogFileAndRunInfo(
                latestProgramRunId,
                latestRunRecordDetail,
                needPipelineLog,
                appFolderPath.toString(),
                metrics,
                threadList,
                statusJson);
          }
        }
      } else if (appId != null) {
        File appFolderPath = new File(basePath.toString(), appId);
        if (!appFolderPath.exists()) {
          appFolderPath.mkdirs();
        }
        statusJson = new FileWriter(appFolderPath + "/" + "status.json");
        Pair<ApplicationId, ApplicationDetail> applicationPair =
            generateApplicationFile(
                namespaceId, appId, appFolderPath.toString(), threadList, statusJson);
        latestProgramRunId =
            new ProgramRunId(namespaceId, appId, ProgramType.WORKFLOW, workflowName, runId);
        latestRunRecordDetail = store.getRun(latestProgramRunId);
        JsonObject metrics =
            queryMetrics(
                namespaceId,
                appId,
                runId,
                workflowName,
                applicationPair.getSecond().getConfiguration(),
                latestRunRecordDetail != null ? latestRunRecordDetail.getStartTs() : 0,
                latestRunRecordDetail != null && latestRunRecordDetail.getStopTs() != null
                    ? latestRunRecordDetail.getStopTs()
                    : DateTime.now().getMillis());
        generateLogFileAndRunInfo(
            latestProgramRunId,
            latestRunRecordDetail,
            needPipelineLog,
            appFolderPath.toString(),
            metrics,
            threadList,
            statusJson);
      }
    }
    if (needSystemLog) {
      File systemLogPath = new File(basePath.getPath(), "system-log");
      if (!systemLogPath.exists()) {
        systemLogPath.mkdirs();
      }
      if (statusJson == null) {
        statusJson = new FileWriter(basePath + "/status.json");
      }
      // Generates system log for user request
      generateSystemLog(systemLogPath.getPath(), threadList, statusJson);
    }
    for (Thread thread : threadList) {
      joinThread(thread);
    }
    if (statusJson != null) {
      statusJson.close();
    }
    return uuid;
  }

  /** Gets oldest folder from the root directory */
  private File getOldestFolder(File baseDirectory) {
    File[] supportFiles = baseDirectory.listFiles();
    long oldestDate = Long.MAX_VALUE;
    File oldestFile = null;
    if (supportFiles != null && supportFiles.length > 0) {
      for (File f : supportFiles) {
        if (f.lastModified() < oldestDate) {
          oldestDate = f.lastModified();
          oldestFile = f;
        }
      }
    }
    return oldestFile;
  }

  /** Deletes old folders after certain number of folders exist */
  private void deleteOldFolders(File oldFilesDirectory) {
    String[] entries = oldFilesDirectory.list();
    if (entries != null && entries.length > 0) {
      for (String s : entries) {
        File currentFile = new File(oldFilesDirectory.getPath(), s);
        // Recursive the full directory and delete all old files
        if (currentFile.isDirectory()) {
          deleteOldFolders(currentFile);
        } else {
          currentFile.delete();
        }
      }
    }
    oldFilesDirectory.delete();
  }

  /** Generates system log */
  private void generateSystemLog(String basePath, List<Thread> threadList, FileWriter statusJson) {
    String pipeline = "pipeline";
    List<String> serviceList =
        Arrays.asList(
            Constants.Service.APP_FABRIC_HTTP,
            Constants.Service.DATASET_EXECUTOR,
            Constants.Service.EXPLORE_HTTP_USER_SERVICE,
            Constants.Service.LOGSAVER,
            Constants.Service.MESSAGING_SERVICE,
            Constants.Service.METADATA_SERVICE,
            Constants.Service.METRICS,
            Constants.Service.METRICS_PROCESSOR,
            Constants.Service.RUNTIME,
            Constants.Service.TRANSACTION,
            pipeline);
    String componentId = "services";
    for (String serviceId : serviceList) {
      final Thread thread =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  try {
                    LoggingContext loggingContext = null;
                    if (serviceId.equals("pipeline")) {
                      loggingContext =
                          LoggingContextHelper.getLoggingContext(
                              "system",
                              serviceId,
                              "studio",
                              ProgramType.valueOfCategoryName(componentId));
                    } else {
                      loggingContext =
                          LoggingContextHelper.getLoggingContext(
                              Id.Namespace.SYSTEM.getId(), componentId, serviceId);
                    }
                    generateLogFile(loggingContext, basePath, serviceId + "-system-log.txt");
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });
      thread.start();
      threadList.add(thread);
      addToStatus(statusJson, serviceId);
    }
  }

  /** Generates application file and return application id and detail */
  private Pair<ApplicationId, ApplicationDetail> generateApplicationFile(
      String namespaceId,
      String appId,
      String appFolderPath,
      List<Thread> threadList,
      FileWriter statusJson)
      throws Exception {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId);
    ApplicationDetail applicationDetail = applicationLifecycleService.getAppDetail(applicationId);
    final Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  FileWriter file = new FileWriter(appFolderPath + "/" + appId + ".json");
                  file.write(GSON.toJson(applicationDetail));
                  file.flush();
                  file.close();
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            });
    thread.start();
    threadList.add(thread);
    addToStatus(statusJson, "applicationFile");
    return new Pair<>(applicationId, applicationDetail);
  }

  /** Gets the metrics details for the run id */
  private JsonObject queryMetrics(
      String namespaceId,
      String appId,
      String runId,
      String workflowName,
      String configuration,
      long startTs,
      long stopTs)
      throws Exception {
    JSONObject appConf = new JSONObject(configuration);
    List<String> metricsList = new ArrayList<>();
    JSONArray stages = appConf.getJSONArray("stages");
    for (int i = 0; i < stages.length(); i++) {
      JSONObject stageName = stages.getJSONObject(i);
      metricsList.add(String.format("user.%s.records.out", stageName.getString("name")));
      metricsList.add(String.format("user.%s.records.in", stageName.getString("name")));
      metricsList.add(String.format("user.%s.process.time.avg", stageName.getString("name")));
    }
    List<String> queryTags = Arrays.asList(namespaceId, appId, runId, workflowName);
    Map<String, List<String>> queryParams = new HashMap<>();
    queryParams.put(
        Constants.AppFabric.QUERY_PARAM_START_TIME,
        Collections.singletonList(String.valueOf(startTs - 5000)));
    queryParams.put(
        Constants.AppFabric.QUERY_PARAM_END_TIME,
        Collections.singletonList(String.valueOf(stopTs)));
    MetricQueryResult metricQueryResult =
        metricsQueryHelper.executeTagQuery(queryTags, metricsList, new ArrayList<>(), queryParams);
    JsonObject metrics = new JsonObject();
    for (MetricQueryResult.TimeSeries timeSeries : metricQueryResult.getSeries()) {
      if (!metrics.has(timeSeries.getMetricName())) {
        metrics.add(timeSeries.getMetricName(), new JsonArray());
      }
      for (MetricQueryResult.TimeValue timeValue : timeSeries.getData()) {
        JsonObject time = new JsonObject();
        time.addProperty("time", timeValue.getTime());
        time.addProperty("value", timeValue.getValue());
        metrics.getAsJsonArray(timeSeries.getMetricName()).add(time);
      }
    }
    return metrics;
  }

  // Zips all the file under support bundle directory */
  //  private void zipFile(String dirPath) {
  //    try {
  //      File zipFile = new File("/Users/bjzhu/Documents/bjzhu/collect_support_bundle.zip");
  //      File confDir = new File(dirPath);
  //      try (ZipOutputStream zipOutput = new ZipOutputStream(new FileOutputStream(zipFile))) {
  //        BundleJarUtil.addToArchive(confDir, zipOutput);
  //      }
  //    } catch (IOException e) {
  //      throw new RuntimeException(e);
  //    }
  //  }

  /** Generates pipeline log file and run info file */
  private void generateLogFileAndRunInfo(
      ProgramRunId latestProgramRunId,
      RunRecordDetail latestRunRecordDetail,
      Boolean needLog,
      String appPath,
      JsonObject metrics,
      List<Thread> threadList,
      FileWriter statusJson) {
    String programType = "workflows";

    if (needLog) {
      // Generates pipeline log file for user request
      final Thread thread =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  try {
                    ProgramType type = ProgramType.valueOfCategoryName(programType);
                    ProgramRunId programRunId =
                        new ProgramRunId(
                            latestProgramRunId.getNamespace(),
                            latestProgramRunId.getApplication(),
                            type,
                            latestProgramRunId.getProgram(),
                            latestProgramRunId.getRun());
                    LoggingContext loggingContext =
                        LoggingContextHelper.getLoggingContextWithRunId(latestProgramRunId, null);
                    generateLogFile(
                        loggingContext, appPath, latestProgramRunId.getRun() + "-log.txt");
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });
      thread.start();
      threadList.add(thread);
      addToStatus(statusJson, "runtimelog");
    }
    // Generate runtime info details file
    final Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  JsonObject jsonObject = new JsonObject();
                  jsonObject.addProperty("status", latestRunRecordDetail.getStatus().toString());
                  jsonObject.addProperty("start", latestRunRecordDetail.getStartTs());
                  jsonObject.addProperty("end", latestRunRecordDetail.getStopTs());
                  jsonObject.addProperty(
                      "profileName", latestRunRecordDetail.getProfileId().getProfile());
                  jsonObject.addProperty(
                      "runtimeArgs", latestRunRecordDetail.getProperties().get("runtimeArgs"));
                  jsonObject.add("metrics", metrics);
                  FileWriter file =
                      new FileWriter(appPath + "/" + latestProgramRunId.getRun() + ".json");
                  file.write(GSON.toJson(jsonObject));
                  file.flush();
                  file.close();
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            });
    thread.start();
    threadList.add(thread);
    addToStatus(statusJson, "runtimeinfo");
  }

  /** Generates log file within certain path */
  private void generateLogFile(LoggingContext loggingContext, String basePath, String filePath)
      throws Exception {
    long currentTimeMillis = System.currentTimeMillis();
    long fromMillis = currentTimeMillis - TimeUnit.DAYS.toMillis(1);
    CloseableIterator<LogEvent> logIter =
        logReader.getLog(loggingContext, fromMillis, currentTimeMillis, FilterParser.parse(""));
    AbstractChunkedLogProducer logsProducer = new TextChunkedLogProducer(logIter, logPattern, true);
    ByteBuf chunk = logsProducer.nextChunk();
    FileWriter file = new FileWriter(basePath + "/" + filePath);
    while (chunk.capacity() > 0) {
      String log = chunk.toString(StandardCharsets.UTF_8);
      file.write(log);
      chunk = logsProducer.nextChunk();
    }
    file.flush();
    file.close();
  }

  /** Adds status info into file */
  private void addToStatus(FileWriter statusJson, String serviceId) {
    try {
      JsonObject serviceJson = new JsonObject();
      serviceJson.addProperty(serviceId, true);
      statusJson.write(serviceJson.toString());
      statusJson.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
