/*
 * Copyright © 2019-2023 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.Security;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.security.server.BasicAuthenticationHandler;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RouterServiceMainWithSecurityEnabledTest extends MasterServiceMainTestBase {

  @BeforeClass
  public static void init() throws Exception {
    final CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Security.ENABLED, true);
    cConf.set(Security.AUTH_HANDLER_CLASS, BasicAuthenticationHandler.class.getName());
    cConf.setBoolean(Security.KERBEROS_ENABLED, false);
    cConf.set(Security.BASIC_REALM_FILE, realmFile());
    MasterServiceMainTestBase.cConf = cConf;

    final SConfiguration sConf = SConfiguration.create();
    sConf.set(Security.AuthenticationServer.SSL_KEYSTORE_PATH, "src/test/resources/KeyStore.jks");
    sConf.set(Security.AuthenticationServer.SSL_KEYSTORE_PASSWORD, "123456");
    MasterServiceMainTestBase.sConf = sConf;

    MasterServiceMainTestBase.init();
  }

  private static String realmFile() throws IOException {
    File tmpFile = File.createTempFile("basicrealm", ".tmp");
    FileWriter writer = new FileWriter(tmpFile);
    writer.write("router: router");
    writer.close();

    return tmpFile.getAbsolutePath();
  }

  @Test
  public void testRouterServiceWithAuthenticationEnabled() throws Exception {
    URL url = getRouterBaseUri().resolve("/").toURL();
    HttpResponse response = HttpRequests
        .execute(HttpRequest.get(url).build(), new DefaultHttpRequestConfig(false));

    Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, response.getResponseCode());
  }
}
