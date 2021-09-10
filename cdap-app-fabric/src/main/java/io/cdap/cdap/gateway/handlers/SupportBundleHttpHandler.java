/*
 * Copyright © 2015-2021 Cask Data, Inc.
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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
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
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import javafx.util.Pair;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/** Support Bundle HTTP Handler. */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class SupportBundleHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleHttpHandler.class);
  private static final Gson GSON =
      ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
          .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
          .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
          .registerTypeAdapter(
              org.apache.twill.internal.Arguments.class,
              new org.apache.twill.internal.json.ArgumentsCodec())
          .create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, Boolean>>() { }.getType();

  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final Store store;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final MetricsQueryHelper metricsQueryHelper;
  private final LogReader logReader;
  private final String logPattern;
  private final List<String> serviceList;
  private final List<String> listOfFileNames;
  private final CConfiguration cConf;
  private final JsonObject serviceJson = new JsonObject();
  private final int folderMaxNumber = 7;

  @Inject
  SupportBundleHttpHandler(
      Store store,
      NamespaceQueryAdmin namespaceQueryAdmin,
      ApplicationLifecycleService applicationLifecycleService,
      MetricsQueryHelper metricsQueryHelper,
      LogReader logReader,
      CConfiguration cConf) {
    this.store = store;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.applicationLifecycleService = applicationLifecycleService;
    this.metricsQueryHelper = metricsQueryHelper;
    this.logReader = logReader;
    this.logPattern =
        cConf.get(LoggingConfiguration.LOG_PATTERN, LoggingConfiguration.DEFAULT_LOG_PATTERN);
    this.serviceList =
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
            "pipeline");
    this.listOfFileNames = new ArrayList<>(serviceList);
    listOfFileNames.addAll(Arrays.asList("applicationFile", "runtimelog", "runtimeinfo"));
    this.cConf = cConf;
    String homePath = System.getProperty("user.home") + "/support/bundle";
    cConf.set(Constants.CFG_LOCAL_DATA_SUPPORT_BUNDLE_DIR, homePath);
  }

  /**
   * Generate the Support Bundle if valid application id, workflow id, and runid are provided.
   *
   * @param namespaceId the namespace id
   * @param appId the app id
   * @param workflowName the workflow name
   * @param runId the runid of the workflow uuid of this support bundle
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
      @Nullable @QueryParam("run-id") String runId) {
    generateSupportBundle(namespaceId, appId, workflowName, runId, responder);
  }

  /** Generates Support Bundle for the given parameters. */
  private void generateSupportBundle(
      String namespaceId,
      String appId,
      String workflowName,
      String runId,
      HttpResponder responder) {
    // Generate a universal unique id for each bundle and return to the front end right away
    UUID uuid = UUID.randomUUID();
    responder.sendString(
        HttpResponseStatus.OK, String.format("Support Bundle %s generated.", uuid));
    try {
      if (namespaceId != null) {
        NamespaceId namespace = new NamespaceId(namespaceId);
        if (!namespaceQueryAdmin.exists(namespace)) {
          throw new NamespaceNotFoundException(namespace);
        }
      }
      ProgramRunId latestProgramRunId = null;
      RunRecordDetail latestRunRecordDetail = null;
      if (workflowName == null) {
        workflowName = "DataPipelineWorkflow";
      }
      List<String> namespaceList = new ArrayList<>();
      // Puts all the files under the uuid path
      File baseDirectory = new File(cConf.get(Constants.CFG_LOCAL_DATA_SUPPORT_BUNDLE_DIR));
      int fileCount = 1;
      DirUtils.mkdirs(baseDirectory);
      if (baseDirectory.list() != null && baseDirectory.list().length > 0) {
        fileCount = baseDirectory.list().length;
      }

      // We want to keep consistent number of bundle to provide to customer
      if (fileCount >= folderMaxNumber) {
        File oldFilesDirectory = getOldestFolder(baseDirectory);
        deleteOldFolders(oldFilesDirectory);
      }
      File basePath =
          new File(cConf.get(Constants.CFG_LOCAL_DATA_SUPPORT_BUNDLE_DIR), uuid.toString());
      DirUtils.mkdirs(basePath);
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
          Predicate<ApplicationDetail> predicate = (detail) -> true;
          apps =
              applicationLifecycleService.getApps(namespace, predicate).stream()
                  .map(ApplicationRecord::new)
                  .collect(Collectors.toList());
        } else {
          apps.add(
              new ApplicationRecord(
                  applicationLifecycleService.getAppDetail(new ApplicationId(namespaceId, appId))));
        }
        File systemLogPath = new File(basePath.getPath(), "system-log");
        DirUtils.mkdirs(systemLogPath);
        // Generates system log for user request
        generateSystemLog(systemLogPath.getPath(), basePath, apps);
        if (runId == null) {
          for (ApplicationRecord app : apps) {
            latestProgramRunId = null;
            latestRunRecordDetail = null;
            File appFolderPath = new File(basePath.toString(), app.getName());
            DirUtils.mkdirs(appFolderPath);
            // Generates application file and get application id and detail
            Pair<ApplicationId, ApplicationDetail> applicationPair =
                generateApplicationFile(namespaceId, app.getName(), appFolderPath.toString());
            Map<ProgramRunId, RunRecordDetail> runMap =
                store.getRuns(
                    applicationPair.getKey(),
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
                      applicationPair.getValue().getConfiguration(),
                      latestRunRecordDetail != null ? latestRunRecordDetail.getStartTs() : 0,
                      latestRunRecordDetail != null && latestRunRecordDetail.getStopTs() != null
                          ? latestRunRecordDetail.getStopTs()
                          : DateTime.now().getMillis());
              generateLogFileAndRunInfo(
                  latestProgramRunId, latestRunRecordDetail, appFolderPath.toString(), metrics);
            }
          }
        } else if (appId != null) {
          File appFolderPath = new File(basePath.toString(), appId);
          DirUtils.mkdirs(appFolderPath);
          Pair<ApplicationId, ApplicationDetail> applicationPair =
              generateApplicationFile(namespaceId, appId, appFolderPath.toString());
          latestProgramRunId =
              new ProgramRunId(namespaceId, appId, ProgramType.WORKFLOW, workflowName, runId);
          latestRunRecordDetail = store.getRun(latestProgramRunId);
          JsonObject metrics =
              queryMetrics(
                  namespaceId,
                  appId,
                  runId,
                  workflowName,
                  applicationPair.getValue().getConfiguration(),
                  latestRunRecordDetail != null ? latestRunRecordDetail.getStartTs() : 0,
                  latestRunRecordDetail != null && latestRunRecordDetail.getStopTs() != null
                      ? latestRunRecordDetail.getStopTs()
                      : DateTime.now().getMillis());
          generateLogFileAndRunInfo(
              latestProgramRunId, latestRunRecordDetail, appFolderPath.toString(), metrics);
        }
      }
    } catch (Exception e) {
      LOG.error("Can not generate support bundle ", e);
    }
  }

  /** Gets oldest folder from the root directory */
  @Nullable
  private File getOldestFolder(File baseDirectory) {
    return DirUtils.listFiles(baseDirectory).stream()
        .reduce((f1, f2) -> f1.lastModified() < f2.lastModified() ? f1 : f2)
        .orElse(null);
  }

  /** Deletes old folders after certain number of folders exist */
  private void deleteOldFolders(File oldFilesDirectory) {
    try {
      DirUtils.deleteDirectoryContents(oldFilesDirectory);
    } catch (IOException e) {
      LOG.warn("Failed to clean up directory {}", oldFilesDirectory, e);
    }
  }

  /** Generates system log */
  private void generateSystemLog(String systemLogPath, File basePath, List<ApplicationRecord> apps)
      throws Exception {
    String componentId = "services";
    for (String serviceId : serviceList) {
      CompletableFuture<Void> futureService =
          CompletableFuture.runAsync(
              () -> {
                if (serviceId.equals("pipeline")) {
                  generateLogFile(
                      LoggingContextHelper.getLoggingContext(
                          "system",
                          serviceId,
                          "studio",
                          ProgramType.valueOfCategoryName(componentId)),
                      systemLogPath,
                      serviceId + "-system-log.txt");
                } else {
                  generateLogFile(
                      LoggingContextHelper.getLoggingContext(
                          Id.Namespace.SYSTEM.getId(), componentId, serviceId),
                      systemLogPath,
                      serviceId + "-system-log.txt");
                }
              });
      futureService.get();
      for (ApplicationRecord app : apps) {
        File appFolderPath = new File(basePath.toString(), app.getName());
        DirUtils.mkdirs(appFolderPath);
        addToStatus(serviceId, appFolderPath.getPath());
      }
    }
  }

  /** Generates application file and return application id and detail */
  private Pair<ApplicationId, ApplicationDetail> generateApplicationFile(
      String namespaceId, String appId, String appFolderPath) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId);
    ApplicationDetail applicationDetail = applicationLifecycleService.getAppDetail(applicationId);
    CompletableFuture<Void> futureApplication =
        CompletableFuture.runAsync(
            () -> {
              try (FileWriter file = new FileWriter(new File(appFolderPath, appId + ".json"))) {
                file.write(GSON.toJson(applicationDetail));
                file.flush();
              } catch (IOException e) {
                LOG.error("Can not write application file ", e);
              }
            });
    futureApplication.get();
    addToStatus("applicationFile", appFolderPath);
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
      long stopTs) {
    try {
      JSONObject appConf =
          configuration != null && configuration.length() > 0
              ? new JSONObject(configuration)
              : new JSONObject();
      List<String> metricsList = new ArrayList<>();
      JSONArray stages = appConf.has("stages") ? appConf.getJSONArray("stages") : new JSONArray();
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
          metricsQueryHelper.executeTagQuery(
              queryTags, metricsList, new ArrayList<>(), queryParams);
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
    } catch (Exception e) {
      LOG.error("Can not add metrics ", e);
      return new JsonObject();
    }
  }

  /** Generates pipeline log file and run info file */
  private void generateLogFileAndRunInfo(
      ProgramRunId latestProgramRunId,
      RunRecordDetail latestRunRecordDetail,
      String appPath,
      JsonObject metrics)
      throws Exception {

    // Generates pipeline log file for user request
    CompletableFuture<Void> futurePipelineLog =
        CompletableFuture.runAsync(
            () -> {
              try {
                LoggingContext loggingContext =
                    LoggingContextHelper.getLoggingContextWithRunId(latestProgramRunId, null);
                generateLogFile(loggingContext, appPath, latestProgramRunId.getRun() + "-log.txt");
              } catch (Exception e) {
                LOG.error("Can not write pipeline log file ", e);
              }
            });
    futurePipelineLog.get();
    addToStatus("runtimelog", appPath);
    // Generate runtime info details file
    CompletableFuture<Void> futureRuntimeInfo =
        CompletableFuture.runAsync(
            () -> {
              try (FileWriter file =
                  new FileWriter(new File(appPath, latestProgramRunId.getRun() + ".json"))) {
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("status", latestRunRecordDetail.getStatus().toString());
                jsonObject.addProperty("start", latestRunRecordDetail.getStartTs());
                jsonObject.addProperty("end", latestRunRecordDetail.getStopTs());
                jsonObject.addProperty(
                    "profileName", latestRunRecordDetail.getProfileId().getProfile());
                jsonObject.addProperty(
                    "runtimeArgs", latestRunRecordDetail.getProperties().get("runtimeArgs"));
                jsonObject.add("metrics", metrics);
                file.write(GSON.toJson(jsonObject));
                file.flush();
              } catch (IOException e) {
                LOG.error("Can not write runtime file ", e);
              }
            });
    futureRuntimeInfo.get();
    addToStatus("runtimeinfo", appPath);
  }

  /** Generates log file within certain path */
  private void generateLogFile(LoggingContext loggingContext, String basePath, String filePath) {
    long currentTimeMillis = System.currentTimeMillis();
    long fromMillis = currentTimeMillis - TimeUnit.DAYS.toMillis(1);
    try (FileWriter file = new FileWriter(new File(basePath, filePath));
        CloseableIterator<LogEvent> logIter =
            logReader.getLog(
                loggingContext, fromMillis, currentTimeMillis, FilterParser.parse(""))) {
      AbstractChunkedLogProducer logsProducer =
          new TextChunkedLogProducer(logIter, logPattern, true);
      ByteBuf chunk = logsProducer.nextChunk();
      while (chunk.capacity() > 0) {
        String log = chunk.toString(StandardCharsets.UTF_8);
        file.write(log);
        chunk = logsProducer.nextChunk();
      }
      file.flush();
    } catch (Exception e) {
      LOG.error("Can not generate log file ", e);
    }
  }

  /** Adds status info into file */
  private synchronized void addToStatus(String serviceId, String basePath) throws IOException {
    try (FileWriter statusJson = new FileWriter(new File(basePath, "status.json"))) {
      serviceJson.addProperty(serviceId, true);
      statusJson.write(serviceJson.toString());
      statusJson.flush();
    }
  }
}
