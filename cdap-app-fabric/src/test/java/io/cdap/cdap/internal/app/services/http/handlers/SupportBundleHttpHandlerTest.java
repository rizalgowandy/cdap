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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.RestartServiceInstancesStatus;
import io.cdap.cdap.proto.SystemServiceMeta;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;

/**
 * Monitor handler tests.
 */
public class SupportBundleHttpHandlerTest extends AppFabricTestBase {

  private HttpURLConnection openURL(String path, HttpMethod method) throws IOException {
    HttpURLConnection urlConn = (HttpURLConnection) createURL(path).openConnection();
    if (urlConn instanceof HttpsURLConnection) {
      new HttpsEnabler().setTrustAll(true).enable((HttpsURLConnection) urlConn);
    }
    urlConn.setRequestMethod(method.name());
    return urlConn;
  }

  private URL createURL(String path) throws MalformedURLException {
    return getEndPoint(String.format("/v3/%s", path)).toURL();
  }

  @Test
  public void testCreateSupportBundleWithValidNamespace() throws Exception {
    createNamespace("default");
    String path = String.format("%s/support/bundle?namespaceId=default", Constants.Gateway.API_VERSION_3);
    HttpResponse response = doPost(path);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }

  @Test
  public void testCreateSupportBundleWithSystemLog() throws Exception {
    createNamespace("default");
    String path = String.format("%s/support/bundle?need-system-log=true", Constants.Gateway.API_VERSION_3);
    HttpResponse response = doPost(path);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Assert.assertTrue(response.getResponseBodyAsString().startsWith("Support Bundle"));
  }
}
