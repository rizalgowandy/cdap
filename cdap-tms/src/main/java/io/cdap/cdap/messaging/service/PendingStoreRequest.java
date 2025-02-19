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

package io.cdap.cdap.messaging.service;

import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.messaging.DefaultStoreRequest;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * A {@link StoreRequest} that represents a pending store request to the underlying storage table.
 */
final class PendingStoreRequest extends DefaultStoreRequest {

  private final StoreRequest originalRequest;
  private final TopicMetadata metadata;

  private boolean completed;
  private long startTimestamp;
  private long endTimestamp;
  private int startSequenceId;
  private int endSequenceId;
  private Throwable failureCause;

  PendingStoreRequest(StoreRequest originalRequest, TopicMetadata topicMetadata) {
    super(originalRequest.getTopicId(), originalRequest.isTransactional(),
        originalRequest.getTransactionWritePointer());
    this.originalRequest = originalRequest;
    this.metadata = topicMetadata;
  }

  TopicMetadata getTopicMetadata() {
    return metadata;
  }

  boolean isCompleted() {
    return completed;
  }

  boolean isSuccess() {
    if (!isCompleted()) {
      throw new IllegalStateException("Write is not yet completed");
    }
    return failureCause == null;
  }

  @Nullable
  Throwable getFailureCause() {
    return failureCause;
  }

  void completed(@Nullable Throwable failureCause) {
    completed = true;
    this.failureCause = failureCause;
  }

  void setStartTimestamp(long startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  void setStartSequenceId(int startSequenceId) {
    this.startSequenceId = startSequenceId;
  }

  void setEndTimestamp(long endTimestamp) {
    this.endTimestamp = endTimestamp;
  }

  void setEndSequenceId(int endSequenceId) {
    this.endSequenceId = endSequenceId;
  }

  long getStartTimestamp() {
    return startTimestamp;
  }

  int getStartSequenceId() {
    return startSequenceId;
  }

  long getEndTimestamp() {
    return endTimestamp;
  }

  int getEndSequenceId() {
    return endSequenceId;
  }

  @Override
  public boolean hasPayload() {
    return originalRequest.hasPayload();
  }

  @Override
  public Iterator<byte[]> iterator() {
    return originalRequest.iterator();
  }

  @Override
  public String toString() {
    return "PendingStoreRequest{"
        + "completed=" + completed
        + ", startTimestamp=" + startTimestamp
        + ", startSequenceId=" + startSequenceId
        + ", endTimestamp=" + endTimestamp
        + ", endSequenceId=" + endSequenceId
        + ", failureCause=" + failureCause
        + '}';
  }
}
