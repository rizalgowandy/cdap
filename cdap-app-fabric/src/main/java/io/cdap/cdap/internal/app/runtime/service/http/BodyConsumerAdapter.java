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

package io.cdap.cdap.internal.app.runtime.service.http;

import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.service.http.HttpContentConsumer;
import io.cdap.cdap.api.service.http.HttpContentProducer;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.http.BodyConsumer;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaders;
import javax.annotation.Nullable;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An adapter class to delegate calls from {@link BodyConsumer} to {@link HttpContentConsumer}.
 */
final class BodyConsumerAdapter extends BodyConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(BodyConsumerAdapter.class);

  private final DelayedHttpServiceResponder responder;
  private final HttpContentConsumer delegate;
  private final ServiceTaskExecutor taskExecutor;
  private final Cancellable contextReleaser;
  private final boolean useTxOnFinish;
  private final boolean useTxOnError;

  private boolean completed;

  /**
   * Constructs a new instance.
   *
   * @param responder the responder used for sending response back to client
   * @param delegate the {@link HttpContentConsumer} to delegate calls to
   * @param taskExecutor a {@link ServiceTaskExecutor} for executing user code
   * @param contextReleaser A {@link Cancellable} for returning the context back to the http
   *     server
   */
  BodyConsumerAdapter(DelayedHttpServiceResponder responder, HttpContentConsumer delegate,
      ServiceTaskExecutor taskExecutor, Cancellable contextReleaser,
      TransactionControl defaultTxControl) {
    this.responder = responder;
    this.delegate = delegate;
    this.taskExecutor = taskExecutor;
    this.contextReleaser = contextReleaser;
    this.useTxOnFinish = Transactions.getTransactionControl(
        defaultTxControl, HttpContentConsumer.class, delegate,
        "onFinish", HttpServiceResponder.class) == TransactionControl.IMPLICIT;
    this.useTxOnError = Transactions.getTransactionControl(
        defaultTxControl, HttpContentConsumer.class, delegate,
        "onError", HttpServiceResponder.class, Throwable.class) == TransactionControl.IMPLICIT;
  }

  @Override
  public void chunk(ByteBuf chunk, HttpResponder responder) {
    // Due to async nature of netty, chunk might get called even we try to close the connection in onError.
    if (completed) {
      return;
    }

    try {
      taskExecutor.execute(
          () -> delegate.onReceived(chunk.nioBuffer(), taskExecutor.getTransactional()), false);
    } catch (Throwable t) {
      onError(t, this.responder);
    }
  }

  @Override
  public void finished(HttpResponder responder) {
    try {
      taskExecutor.execute(() -> delegate.onFinish(BodyConsumerAdapter.this.responder),
          useTxOnFinish);
    } catch (Throwable t) {
      onError(t, this.responder);
      return;
    }

    // To the HttpContentConsumer, the call is completed even if it fails to send response back to client.
    completed = true;
    try {
      BodyConsumerAdapter.this.responder.execute();
    } finally {
      taskExecutor.releaseCallResources();
      if (!this.responder.hasContentProducer()) {
        contextReleaser.cancel();
      }
    }
  }

  @Override
  public void handleError(Throwable cause) {
    // When this method is called from netty-http, the response has already been sent, hence uses a no-op
    // DelayedHttpServiceResponder for the onError call.
    onError(cause, new DelayedHttpServiceResponder(responder, new ErrorBodyProducerFactory()) {
      @Override
      protected void doSend(int status, String contentType,
          @Nullable ByteBuf content,
          @Nullable HttpContentProducer contentProducer,
          @Nullable HttpHeaders headers) {
        // no-op
      }

      @Override
      public void setFailure(Throwable t) {
        // no-op
      }

      @Override
      public void execute(boolean keepAlive) {
        // no-op
      }

      @Override
      public boolean hasContentProducer() {
        // Always release the context at the end since it's not possible to send with a content producer
        return false;
      }
    });
  }

  /**
   * Calls the {@link HttpContentConsumer#onError(HttpServiceResponder, Throwable)} method from a
   * transaction.
   */
  private void onError(Throwable cause, DelayedHttpServiceResponder responder) {
    if (completed) {
      return;
    }

    // To the HttpContentConsumer, once onError is called, no other methods will be triggered
    completed = true;
    try {
      taskExecutor.execute(() -> delegate.onError(responder, cause), useTxOnError);
    } catch (Throwable t) {
      responder.setFailure(t);
      LOG.warn("Exception in calling HttpContentConsumer.onError", t);
    } finally {
      try {
        responder.execute(false);
      } finally {
        taskExecutor.releaseCallResources();
        if (!responder.hasContentProducer()) {
          contextReleaser.cancel();
        }
      }
    }
  }

  /**
   * A {@link BodyProducerFactory} to be used when {@link #handleError(Throwable)} is called.
   */
  private static final class ErrorBodyProducerFactory implements BodyProducerFactory {

    @Override
    public BodyProducer create(HttpContentProducer contentProducer,
        ServiceTaskExecutor taskExecutor) {
      // It doesn't matter what it returns as it'll never get used
      // Returning a body producer that gives empty content
      return new BodyProducer() {
        @Override
        public ByteBuf nextChunk() throws Exception {
          return Unpooled.EMPTY_BUFFER;
        }

        @Override
        public void finished() throws Exception {
          // no-op
        }

        @Override
        public void handleError(@Nullable Throwable throwable) {
          // no-op
        }
      };
    }
  }
}
