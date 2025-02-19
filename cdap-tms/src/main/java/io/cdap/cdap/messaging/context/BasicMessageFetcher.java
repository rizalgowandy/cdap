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

package io.cdap.cdap.messaging.context;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.DefaultMessageFetchRequest;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;

/**
 * Implementation of {@link MessageFetcher} that implements {@link TransactionAware}. The active
 * transaction will be used for fetching messages if there is one. Otherwise messages will be
 * fetched without transaction.
 */
final class BasicMessageFetcher implements MessageFetcher, TransactionAware {

  private final MessagingService messagingService;
  private final String name;
  private Transaction transaction;

  BasicMessageFetcher(MessagingService messagingService) {
    this.messagingService = messagingService;
    this.name = "MessageFetcher-" + Thread.currentThread().getName();
  }

  @Override
  public CloseableIterator<Message> fetch(String namespace, String topic,
      int limit, long timestamp) throws IOException, TopicNotFoundException {
    DefaultMessageFetchRequest.Builder fetchRequestBuilder =
        new DefaultMessageFetchRequest.Builder()
            .setTopicId(new NamespaceId(namespace).topic(topic))
            .setLimit(limit)
            .setStartTime(timestamp);

    if (transaction != null) {
      fetchRequestBuilder.setTransaction(transaction);
    }

    return new MessageIterator(messagingService.fetch(fetchRequestBuilder.build()));
  }

  @Override
  public CloseableIterator<Message> fetch(
      String namespace, String topic, int limit, @Nullable String afterMessageId)
      throws IOException, TopicNotFoundException {
    DefaultMessageFetchRequest.Builder fetchRequestBuilder =
        new DefaultMessageFetchRequest.Builder()
            .setTopicId(new NamespaceId(namespace).topic(topic))
            .setLimit(limit);

    if (afterMessageId != null) {
      fetchRequestBuilder.setStartMessage(Bytes.fromHexString(afterMessageId), false);
    }

    if (transaction != null) {
      fetchRequestBuilder.setTransaction(transaction);
    }

    return new MessageIterator(messagingService.fetch(fetchRequestBuilder.build()));
  }

  @Override
  public void startTx(Transaction transaction) {
    this.transaction = transaction;
  }

  @Override
  public void updateTx(Transaction transaction) {
    // Currently CDAP doesn't support checkpoint.
    throw new UnsupportedOperationException("Transaction checkpoints are not supported");
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return Collections.emptySet();
  }

  @Override
  public boolean commitTx() throws Exception {
    return true;
  }

  @Override
  public void postTxCommit() {
    transaction = null;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    transaction = null;
    return true;
  }

  @Override
  public String getTransactionAwareName() {
    return name;
  }
}
