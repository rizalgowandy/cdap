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

package io.cdap.cdap.internal.tethering.runtime.spi.runtimejob;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.tethering.TetheringControlMessage;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringConf;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringProvisioner;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobDetail;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobInfo;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tethering runtime job manager. This class sends runtime job details to the control channel, which will be passed
 * along for the tethered CDAP instance to run.
 * An instance of this class is created by {@link TetheringProvisioner}.
 */
public class TetheringRuntimeJobManager implements RuntimeJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(TetheringRuntimeJobManager.class);
  private static final Gson GSON = new GsonBuilder().create();

  private final String tetheredInstanceName;
  private final String tetheredNamespace;
  private final MessagePublisher messagePublisher;
  private final TopicId topicId;

  public TetheringRuntimeJobManager(TetheringConf conf, CConfiguration cConf, MessagingService messagingService) {
    this.tetheredInstanceName = conf.getTetheredInstanceName();
    this.tetheredNamespace = conf.getTetheredNamespace();
    this.messagePublisher = new MultiThreadMessagingContext(messagingService).getMessagePublisher();
    this.topicId = new TopicId(NamespaceId.SYSTEM.getNamespace(),
                               cConf.get(Constants.Tethering.TOPIC_PREFIX) + tetheredInstanceName);
  }

  @Override
  public void launch(RuntimeJobInfo runtimeJobInfo) throws Exception {
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    LOG.debug("Launching run {} with following configurations: tethered instance name {}, tethered namespace {}.",
              runInfo.getRun(), tetheredInstanceName, tetheredNamespace);
    byte[] payload = Bytes.toBytes(GSON.toJson(createPayload()));
    TetheringControlMessage message = new TetheringControlMessage(TetheringControlMessage.Type.RUN_PIPELINE, payload);
    publishToControlChannel(message);
  }

  @Override
  public Optional<RuntimeJobDetail> getDetail(ProgramRunInfo programRunInfo) {
    // TODO: CDAP-18739 - pull job status instead of always treating it as RUNNING
    return Optional.of(new RuntimeJobDetail(programRunInfo, RuntimeJobStatus.RUNNING));
  }

  @Override
  public List<RuntimeJobDetail> list() throws Exception {
    // TODO: CDAP-18739 - pull list of all running jobs in tethered instance. This method is unused
    return new ArrayList<>();
  }

  @Override
  public void stop(ProgramRunInfo programRunInfo) throws Exception {
    RuntimeJobDetail jobDetail = getDetail(programRunInfo).orElse(null);
    if (jobDetail == null) {
      return;
    }
    RuntimeJobStatus status = jobDetail.getStatus();
    if (status.isTerminated() || status == RuntimeJobStatus.STOPPING) {
      return;
    }
    LOG.debug("Stopping run {} with following configurations: tethered instance name {}, tethered namespace {}.",
              programRunInfo.getRun(), tetheredInstanceName, tetheredNamespace);
    byte[] payload = Bytes.toBytes(GSON.toJson(programRunInfo));
    TetheringControlMessage message = new TetheringControlMessage(TetheringControlMessage.Type.STOP_PIPELINE, payload);
    publishToControlChannel(message);
  }

  @Override
  public void kill(ProgramRunInfo programRunInfo) throws Exception {
    stop(programRunInfo);
  }

  @Override
  public void close() {
    // no-op
  }

  // TODO: create payload containing necessary files
  private Map<String, String> createPayload() {
    return null;
  }

  @VisibleForTesting
  void publishToControlChannel(TetheringControlMessage message) throws Exception {
    try {
      messagePublisher.publish(topicId.getNamespace(), topicId.getTopic(), StandardCharsets.UTF_8,
                               GSON.toJson(message));
    } catch (IOException | TopicNotFoundException e) {
      throw new Exception(String.format("Failed to publish to topic %s", topicId), e);
    }
  }
}
