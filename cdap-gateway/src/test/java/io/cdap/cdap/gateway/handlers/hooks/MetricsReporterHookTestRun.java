/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.hooks;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.gateway.GatewayFastTestsSuite;
import io.cdap.cdap.gateway.GatewayTestBase;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test MetricReporterHook.
 */
public class MetricsReporterHookTestRun extends GatewayTestBase {
  private static final String API_KEY = "SampleTestApiKey";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.API_KEY, API_KEY);

  @Test
  public void testMetricsSuccess() throws Exception {
    String context = "tag=namespace:system&tag=component:appfabric&tag=handler:PingHandler&tag=method:ping";

    // todo: better fix needed: CDAP-2174
    TimeUnit.SECONDS.sleep(1);

    long received = getMetricValue(context, "system.request.received");
    long successful = getMetricValue(context, "system.response.successful");
    long clientError = getMetricValue(context, "system.response.client-error");

    // Make a successful call
    HttpResponse response = GatewayFastTestsSuite.doGet("/ping");
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());

    // received and successful should have increased by one, clientError should be the same
    verifyMetrics(received + 1, context, "system.request.received");
    verifyMetrics(successful + 1, context, "system.response.successful");
    verifyMetrics(clientError, context, "system.response.client-error");
  }

  /**
   * Verify metrics. It tries couple times to avoid race condition.
   * This is because metrics hook is updated asynchronously.
   */
  private void verifyMetrics(long expected, final String context, final String metricsName) throws Exception {
    Tasks.waitFor(expected, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return getMetricValue(context, metricsName);
      }
    }, 10L, TimeUnit.SECONDS, 100L, TimeUnit.MILLISECONDS);
  }

  private static long getMetricValue(String context, String metricName) throws Exception {
    HttpResponse response = doPost("/v3/metrics/query?" + context + "&metric=" + metricName);
    Assert.assertEquals("POST " + context + " did not return 200 status.",
                        HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    // response has the form:
    // {"startTime":0,"endTime":...,"series":[{"metricName":"...","grouping":{},"data":[{"time":0,"value":<value>}]}]}
    JsonObject json = new Gson().fromJson(content, JsonObject.class);
    JsonArray array = json.getAsJsonArray("series");
    if (array.size() > 0) {
      array = array.get(0).getAsJsonObject().getAsJsonArray("data");
      if (array.size() > 0) {
        return array.get(0).getAsJsonObject().get("value").getAsLong();
      }
    }
    return 0L;
  }

  private static HttpResponse doPost(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(getEndPoint(resource));
    post.setHeader(AUTH_HEADER);
    return client.execute(post);
  }
}
