/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.provision;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.app.program.ProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import java.net.URI;
import java.util.Map;
import javax.inject.Inject;

/**
 * Sends notifications about program run provisioner operations.
 */
public class ProvisionerNotifier {

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
      new GsonBuilder()).create();
  private final ProgramStatePublisher publisher;

  @Inject
  ProvisionerNotifier(MessagingProgramStatePublisher publisher) {
    this.publisher = publisher;
  }

  public void provisioning(ProgramRunId programRunId, ProgramOptions programOptions,
      ProgramDescriptor programDescriptor, String userId) {
    publish(ImmutableMap.<String, String>builder()
        .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
        .put(ProgramOptionConstants.PROGRAM_DESCRIPTOR, GSON.toJson(programDescriptor))
        .put(ProgramOptionConstants.USER_ID, userId)
        .put(ProgramOptionConstants.CLUSTER_STATUS, ProgramRunClusterStatus.PROVISIONING.name())
        .put(ProgramOptionConstants.DEBUG_ENABLED, String.valueOf(programOptions.isDebug()))
        .put(ProgramOptionConstants.USER_OVERRIDES,
            GSON.toJson(programOptions.getUserArguments().asMap()))
        .put(ProgramOptionConstants.SYSTEM_OVERRIDES,
            GSON.toJson(programOptions.getArguments().asMap()))
        .put(ProgramOptionConstants.ARTIFACT_ID,
            GSON.toJson(programDescriptor.getArtifactId().toApiArtifactId()))
        .build());
  }

  public void provisioned(ProgramRunId programRunId, ProgramOptions programOptions,
      ProgramDescriptor programDescriptor,
      String userId, Cluster cluster, URI secureKeysDir) {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
        .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
        .put(ProgramOptionConstants.PROGRAM_DESCRIPTOR, GSON.toJson(programDescriptor))
        .put(ProgramOptionConstants.USER_ID, userId)
        .put(ProgramOptionConstants.CLUSTER_STATUS, ProgramRunClusterStatus.PROVISIONED.name())
        .put(ProgramOptionConstants.CLUSTER, GSON.toJson(cluster))
        .put(ProgramOptionConstants.DEBUG_ENABLED, String.valueOf(programOptions.isDebug()))
        .put(ProgramOptionConstants.USER_OVERRIDES,
            GSON.toJson(programOptions.getUserArguments().asMap()))
        .put(ProgramOptionConstants.SYSTEM_OVERRIDES,
            GSON.toJson(programOptions.getArguments().asMap()))
        .put(ProgramOptionConstants.SECURE_KEYS_DIR, GSON.toJson(secureKeysDir))
        .build();

    publish(properties);
  }

  public void deprovisioning(ProgramRunId programRunId) {
    publish(ImmutableMap.of(
        ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId),
        ProgramOptionConstants.CLUSTER_STATUS, ProgramRunClusterStatus.DEPROVISIONING.name()));
  }

  public void deprovisioned(ProgramRunId programRunId) {
    deprovisioned(programRunId, System.currentTimeMillis());
  }

  // this time stamp is in unit MILLISECOND
  public void deprovisioned(ProgramRunId programRunId, long endTimestamp) {
    publish(ImmutableMap.of(
        ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId),
        ProgramOptionConstants.CLUSTER_STATUS, ProgramRunClusterStatus.DEPROVISIONED.name(),
        ProgramOptionConstants.CLUSTER_END_TIME, String.valueOf(endTimestamp)));
  }

  public void orphaned(ProgramRunId programRunId) {
    orphaned(programRunId, System.currentTimeMillis());
  }

  // this time stamp is in unit MILLISECOND
  public void orphaned(ProgramRunId programRunId, long endTimestamp) {
    publish(ImmutableMap.of(
        ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId),
        ProgramOptionConstants.CLUSTER_STATUS, ProgramRunClusterStatus.ORPHANED.name(),
        ProgramOptionConstants.CLUSTER_END_TIME, String.valueOf(endTimestamp)));
  }

  private void publish(Map<String, String> properties) {
    publisher.publish(Notification.Type.PROGRAM_STATUS, properties);
  }
}
