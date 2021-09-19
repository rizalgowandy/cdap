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

package io.cdap.cdap.internal.app;

import com.google.common.collect.Iterators;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.internal.app.worker.TaskWorkerHttpHandlerInternal;
import io.cdap.cdap.metrics.collect.AggregatedMetricsCollectionService;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tests for RemoteTaskExecutor
 */
public class RemoteTaskExecutorTest {

  private static RemoteClientFactory remoteClientFactory;
  private static CConfiguration cConf;
  private static NettyHttpService httpService;
  private List<MetricValues> published;
  private AggregatedMetricsCollectionService mockMetricsCollector;

  public void beforeTest(boolean startHttpService) throws Exception {
    published = new ArrayList<>();
    cConf = CConfiguration.create();
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    remoteClientFactory = new RemoteClientFactory(discoveryService,
                                                  new DefaultInternalAuthenticator(new AuthenticationTestContext()));
    if (startHttpService) {
      httpService = new CommonNettyHttpServiceBuilder(cConf, "test")
        .setHttpHandlers(
          new TaskWorkerHttpHandlerInternal(cConf, className -> {
          }, new NoOpMetricsCollectionService())
        )
        .setPort(cConf.getInt(Constants.ArtifactLocalizer.PORT))
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        })
        .build();
      httpService.start();
      discoveryService.register(URIScheme.createDiscoverable(Constants.Service.TASK_WORKER, httpService));
    }
    mockMetricsCollector = new AggregatedMetricsCollectionService(10L) {
      @Override
      protected void publish(Iterator<MetricValues> metrics) {
        Iterators.addAll(published, metrics);
      }
    };
    mockMetricsCollector.startAndWait();
  }

  @Test
  public void testFailedMetrics() throws Exception {
    beforeTest(true);
    RemoteTaskExecutor remoteTaskExecutor = new RemoteTaskExecutor(cConf, mockMetricsCollector, remoteClientFactory);
    RunnableTaskRequest runnableTaskRequest = RunnableTaskRequest.getBuilder(InValidRunnableClass.class.getName()).
      withParam("param").build();
    try {
      remoteTaskExecutor.runTask(runnableTaskRequest);
    } catch (Exception e) {

    }
    mockMetricsCollector.stopAndWait();
    Assert.assertSame(1, published.size());

    //check the metrics are present
    MetricValues metricValues = published.get(0);
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.CLIENT_REQUEST_LATENCY_MS));
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.CLIENT_REQUEST_COUNT));
    //check the clz tag is set correctly
    Assert.assertEquals(InValidRunnableClass.class.getName(), metricValues.getTags().get("clz"));
    cleanup();
  }

  @Test
  public void testSuccessMetrics() throws Exception {
    beforeTest(true);
    RemoteTaskExecutor remoteTaskExecutor = new RemoteTaskExecutor(cConf, mockMetricsCollector, remoteClientFactory);
    RunnableTaskRequest runnableTaskRequest = RunnableTaskRequest.getBuilder(ValidRunnableClass.class.getName()).
      withParam("param").build();
    remoteTaskExecutor.runTask(runnableTaskRequest);
    mockMetricsCollector.stopAndWait();
    Assert.assertSame(1, published.size());

    //check the metrics are present
    MetricValues metricValues = published.get(0);
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.CLIENT_REQUEST_LATENCY_MS));
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.CLIENT_REQUEST_COUNT));
    //check the clz tag is set correctly
    Assert.assertEquals(ValidRunnableClass.class.getName(), metricValues.getTags().get("clz"));
    cleanup();
  }

  @Test
  public void testRetryMetrics() throws Exception {
    beforeTest(false);
    RemoteTaskExecutor remoteTaskExecutor = new RemoteTaskExecutor(cConf, mockMetricsCollector, remoteClientFactory);
    RunnableTaskRequest runnableTaskRequest = RunnableTaskRequest.getBuilder(ValidRunnableClass.class.getName()).
      withParam("param").build();
    try {
      remoteTaskExecutor.runTask(runnableTaskRequest);
    } catch (Exception e) {

    }
    mockMetricsCollector.stopAndWait();
    Assert.assertSame(1, published.size());

    //check the metrics are present
    MetricValues metricValues = published.get(0);
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.CLIENT_REQUEST_COUNT));
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.CLIENT_REQUEST_LATENCY_MS));
    Assert.assertEquals("failure", metricValues.getTags().get(Constants.Metrics.Tag.STATUS));
    int retryCount = Integer.parseInt(metricValues.getTags().get(Constants.Metrics.Tag.TRIES));
    Assert.assertTrue(retryCount > 1);
  }

  private boolean hasMetric(MetricValues metricValues, String metricName) {
    for (MetricValue metricValue : metricValues.getMetrics()) {
      if (metricValue.getName().equals(metricName)) {
        return true;
      }
    }
    return false;
  }

  public static void cleanup() throws Exception {
    httpService.stop();
  }

  static class ValidRunnableClass implements RunnableTask {
    @Override
    public void run(RunnableTaskContext context) throws Exception {
      context.writeResult("success".getBytes(StandardCharsets.UTF_8));
    }
  }

  static class InValidRunnableClass implements RunnableTask {
    @Override
    public void run(RunnableTaskContext context) throws Exception {
      throw new RuntimeException("Invalid");
    }
  }
}
