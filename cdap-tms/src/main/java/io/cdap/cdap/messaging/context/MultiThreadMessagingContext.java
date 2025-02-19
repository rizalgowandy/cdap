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

import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.common.transaction.MultiThreadTransactionAware;
import io.cdap.cdap.messaging.spi.MessagingService;
import org.apache.tephra.TransactionAware;

/**
 * The basic implementation of {@link MessagingContext} for supporting message publishing/fetching
 * in both non-transactional context and short transaction context (hence not for MR and Spark).
 *
 * Instance of this class can be added as an extra {@link TransactionAware} to the program context
 * so that it can tap into all transaction lifecycle events across all threads.
 */
public class MultiThreadMessagingContext extends MultiThreadTransactionAware<BasicMessagingContext>
    implements MessagingContext {

  private final MessagingService messagingService;

  public MultiThreadMessagingContext(final MessagingService messagingService) {
    this.messagingService = messagingService;
  }

  @Override
  public MessagePublisher getMessagePublisher() {
    return getCurrentThreadTransactionAware().getPublisher();
  }

  @Override
  public MessagePublisher getDirectMessagePublisher() {
    return new DirectMessagePublisher(messagingService);
  }

  @Override
  public MessageFetcher getMessageFetcher() {
    return getCurrentThreadTransactionAware().getFetcher();
  }

  @Override
  protected BasicMessagingContext createTransactionAwareForCurrentThread() {
    return new BasicMessagingContext(messagingService);
  }
}
