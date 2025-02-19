/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.messaging.server;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.ByteBuffers;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.messaging.DefaultMessageFetchRequest;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.Schemas;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.tephra.TransactionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A netty http handler for handling message fetching REST API for the messaging system.
 */
@Path("/v1/namespaces/{namespace}/topics/{topic}")
public final class FetchHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FetchHandler.class);
  // Log at most once per minute.
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(60000));
  private static final TransactionCodec TRANSACTION_CODEC = new TransactionCodec();
  private static final Set<String> KNOWN_IO_EXCEPTION_MESSAGES = ImmutableSet.of(
      "Connection reset by peer",
      "Broken pipe"
  );

  private final MessagingService messagingService;
  private int messageChunkSize;

  @Inject
  FetchHandler(CConfiguration cConf, MessagingService messagingService) {
    this.messagingService = messagingService;
    this.messageChunkSize = cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_CONSUME_CHUNK_SIZE);
  }

  @POST
  @Path("poll")
  public void poll(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace") String namespace,
      @PathParam("topic") String topic) throws Exception {

    TopicId topicId = new NamespaceId(namespace).topic(topic);

    // Currently only support avro
    if (!"avro/binary".equals(request.headers().get(HttpHeaderNames.CONTENT_TYPE))) {
      throw new BadRequestException("Only avro/binary content type is supported.");
    }

    // Decode the poll request
    Decoder decoder = DecoderFactory.get()
        .directBinaryDecoder(new ByteBufInputStream(request.content()), null);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(
        Schemas.V1.ConsumeRequest.SCHEMA);

    // Fetch the messages
    CloseableIterator<RawMessage> iterator = fetchMessages(datumReader.read(null, decoder),
        topicId);
    try {
      responder.sendContent(HttpResponseStatus.OK,
          new MessagesBodyProducer(iterator, messageChunkSize),
          new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "avro/binary"));
    } catch (Throwable t) {
      iterator.close();
      throw t;
    }
  }

  /** Creates a {@link CloseableIterator} of {@link RawMessage} based on the given fetch request. */
  private CloseableIterator<RawMessage> fetchMessages(GenericRecord fetchRequest, TopicId topicId)
      throws IOException, TopicNotFoundException {
    DefaultMessageFetchRequest.Builder fetchRequestBuilder =
        new DefaultMessageFetchRequest.Builder();

    fetchRequestBuilder.setTopicId(topicId);

    Object startFrom = fetchRequest.get("startFrom");
    if (startFrom != null) {
      if (startFrom instanceof ByteBuffer) {
        // start message id is specified
        fetchRequestBuilder.setStartMessage(
            Bytes.toBytes((ByteBuffer) startFrom), (Boolean) fetchRequest.get("inclusive"));
      } else if (startFrom instanceof Long) {
        // start by timestamp is specified
        fetchRequestBuilder.setStartTime((Long) startFrom);
      } else {
        // This shouldn't happen as it's guaranteed by the schema
        LOG.warn(
            "Ignore unrecognized type for startFrom. Type={}, Value={}",
            startFrom.getClass(),
            startFrom);
      }
    }

    Integer limit = (Integer) fetchRequest.get("limit");
    if (limit != null) {
      fetchRequestBuilder.setLimit(limit);
    }

    ByteBuffer encodedTx = (ByteBuffer) fetchRequest.get("transaction");
    if (encodedTx != null) {
      fetchRequestBuilder.setTransaction(
          TRANSACTION_CODEC.decode(ByteBuffers.getByteArray(encodedTx)));
    }

    return messagingService.fetch(fetchRequestBuilder.build());
  }

  /**
   * A {@link BodyProducer} to encode and send back messages. Instead of using GenericDatumWriter,
   * we perform the array encoding manually so that we don't have to buffer all messages in memory
   * before sending out.
   */
  private static class MessagesBodyProducer extends BodyProducer {

    private final CloseableIterator<RawMessage> iterator;
    private final List<RawMessage> messages;
    private final int messageChunkSize;
    private final ByteBuf chunk;
    private final Encoder encoder;
    private final GenericRecord messageRecord;
    private final DatumWriter<GenericRecord> messageWriter;
    private boolean arrayStarted;
    private boolean arrayEnded;

    MessagesBodyProducer(CloseableIterator<RawMessage> iterator, int messageChunkSize) {
      this.iterator = iterator;
      this.messages = new ArrayList<>();
      this.messageChunkSize = messageChunkSize;
      this.chunk = Unpooled.buffer(messageChunkSize);
      this.encoder = EncoderFactory.get().directBinaryEncoder(new ByteBufOutputStream(chunk), null);

      // These are for writing individual message (response is an array of messages)
      this.messageRecord = new GenericData.Record(
          Schemas.V1.ConsumeResponse.SCHEMA.getElementType());
      this.messageWriter = new GenericDatumWriter<GenericRecord>(
          Schemas.V1.ConsumeResponse.SCHEMA.getElementType()) {
        @Override
        protected void writeBytes(Object datum, Encoder out) throws IOException {
          if (datum instanceof byte[]) {
            out.writeBytes((byte[]) datum);
          } else {
            super.writeBytes(datum, out);
          }
        }
      };
    }

    @Override
    public ByteBuf nextChunk() throws Exception {
      // Already sent all messages, return empty to signal the end of response
      if (arrayEnded) {
        return Unpooled.EMPTY_BUFFER;
      }

      chunk.clear();

      if (!arrayStarted) {
        arrayStarted = true;
        encoder.writeArrayStart();
      }

      // Try to buffer up to buffer size
      int size = 0;
      messages.clear();
      while (iterator.hasNext() && size < messageChunkSize) {
        RawMessage message = iterator.next();
        messages.add(message);

        // Avro encodes bytes as (len + bytes), hence adding 8 to cater for the length of the id and payload
        // Straightly speaking it can be up to 9 bytes each (hence 18 bytes),
        // but we don't expect id and payload of such size
        size += message.getId().length + message.getPayload().length + 8;
      }

      encoder.setItemCount(messages.size());
      for (RawMessage message : messages) {
        encoder.startItem();

        // Write individual message (array element) with DatumWrite.
        // This provides greater flexibility on schema evolution.
        // The response will likely always be an array, but the element schema can evolve.
        messageRecord.put("id", message.getId());
        messageRecord.put("payload", message.getPayload());
        messageWriter.write(messageRecord, encoder);
      }

      if (!iterator.hasNext()) {
        arrayEnded = true;
        encoder.writeArrayEnd();
      }

      return chunk.copy();
    }

    @Override
    public void finished() throws Exception {
      iterator.close();
      chunk.release();
    }

    @Override
    public void handleError(@Nullable Throwable cause) {
      iterator.close();
      // Since response header is already sent, there is nothing we can send back to client. Simply log the failure
      if (cause instanceof SocketException
          || cause instanceof ClosedChannelException
          || (cause instanceof IOException && KNOWN_IO_EXCEPTION_MESSAGES.contains(
          cause.getMessage()))) {
        // This can easily caused by client close connection prematurely. Don't want to flood the log.
        LOG.trace("Connection closed by client prematurely while sending messages back to client",
            cause);
      } else {
        // Use sampling logger to log to avoid flooding the log if there is any systematic failure
        SAMPLING_LOG.warn("Exception raised when sending messages back to client", cause);
        // Also log a trace to provide a way to see every error if needed
        LOG.trace("Exception raised when sending messages back to client", cause);
      }
    }
  }
}
