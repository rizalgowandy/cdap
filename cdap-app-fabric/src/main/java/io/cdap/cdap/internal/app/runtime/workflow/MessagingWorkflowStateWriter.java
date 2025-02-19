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

package io.cdap.cdap.internal.app.runtime.workflow;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;

/**
 * An implementation of {@link WorkflowStateWriter} that writes to TMS.
 */
public class MessagingWorkflowStateWriter implements WorkflowStateWriter {

  private static final Gson GSON = new Gson();

  private final TopicId topic;
  private final MessagingService messagingService;
  private final RetryStrategy retryStrategy;

  @Inject
  MessagingWorkflowStateWriter(CConfiguration cConf, MessagingService messagingService) {
    this.topic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Metadata.MESSAGING_TOPIC));
    this.messagingService = messagingService;
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.metadata.");
  }

  @Override
  public void setWorkflowToken(ProgramRunId workflowRunId, WorkflowToken token) {
    MetadataMessage message = new MetadataMessage(MetadataMessage.Type.WORKFLOW_TOKEN,
        workflowRunId, GSON.toJsonTree(token));
    StoreRequest request = StoreRequestBuilder.of(topic).addPayload(GSON.toJson(message)).build();
    try {
      Retries.callWithRetries(() -> messagingService.publish(request), retryStrategy,
          Retries.ALWAYS_TRUE);
    } catch (Exception e) {
      // Don't log the workflow token, as it can be large and may contain sensitive data
      throw new RuntimeException(
          "Failed to publish workflow token for workflow run " + workflowRunId, e);
    }
  }

  @Override
  public void addWorkflowNodeState(ProgramRunId workflowRunId, WorkflowNodeStateDetail state) {
    MetadataMessage message = new MetadataMessage(MetadataMessage.Type.WORKFLOW_STATE,
        workflowRunId, GSON.toJsonTree(state));
    StoreRequest request = StoreRequestBuilder.of(topic).addPayload(GSON.toJson(message)).build();
    try {
      Retries.callWithRetries(() -> messagingService.publish(request), retryStrategy,
          Retries.ALWAYS_TRUE);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to publish workflow node state for workflow run " + workflowRunId
              + "of node " + state.getNodeId() + " with state " + state.getNodeStatus(), e);
    }
  }
}
