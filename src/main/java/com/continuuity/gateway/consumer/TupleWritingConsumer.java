package com.continuuity.gateway.consumer;

import com.continuuity.api.data.WriteOperation;
import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.Consumer;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TupleWritingConsumer extends Consumer {

  /**
   * This is the operations executor that we will use to talk to the data-fabric
   */
  @Inject
  private OperationExecutor executor;

  /**
   * This is our Logger class
   */
  private static final Logger LOG = LoggerFactory
      .getLogger(TupleWritingConsumer.class);

  /**
   * Use this if you don't use Guice to create the consumer
   *
   * @param executor The operations executor to use
   */
  public void setExecutor(OperationExecutor executor) {
    this.executor = executor;
  }

  /**
   * To avoid the overhead of creating new serializer for every event,
   * we keep a serializer for each thread in a thread local structure.
   */
  ThreadLocal<TupleSerializer> serializers =
      new ThreadLocal<TupleSerializer>();

  /**
   * Utility method to get or create the thread local serializer
   */
  TupleSerializer getSerializer() {
    if (this.serializers.get() == null) {
      this.serializers.set(new TupleSerializer(false));
    }
    return this.serializers.get();
  }

  @Override
  protected void single(Event event) throws Exception {
    this.batch(Collections.singletonList(event));
  }

  @Override
  protected void batch(List<Event> events) throws Exception {
    List<WriteOperation> operations = new ArrayList<WriteOperation>(events.size());
    TupleSerializer serializer = getSerializer();
    for (Event event : events) {
      // convert the event into a tuple
      Tuple tuple = new TupleBuilder().
          set("headers", event.getHeaders()).
          set("body", event.getBody()).
          create();
      // and serialize it
      byte[] bytes = serializer.serialize(tuple);
      if (bytes == null) {
        Exception e = new Exception("Could not serialize event: " + event);
        LOG.warn("Could not serialize event: " + event, e);
        throw e;
      }
      // figure out where to write it
      String destination = event.getHeader(Constants.HEADER_DESTINATION_STREAM);
      if (destination == null) {
        LOG.debug("Enqueuing an event that has no destination. " +
            "Using 'default' instead.");
        destination = "default";
      }
      // construct the stream URI to use for the data fabric
      String queueURI = FlowStream.buildStreamURI(destination).toString();
      operations.add(new QueueEnqueue(queueURI.getBytes(), bytes));
      LOG.debug("Sending tuple to " + queueURI + ", tuple = " + event);

    }
    try {
      this.executor.execute(operations);
    } catch (Exception e) {
      Exception e1 = new Exception(
          "Failed to enqueue event(s): " + e.getMessage(), e);
      LOG.error(e.getMessage(), e);
      throw e1;
    }
  }

}
