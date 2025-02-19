/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.security.server;

import com.unboundid.ldap.listener.InMemoryListenerConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link ExternalAuthenticationServer}.
 */
public class ExternalLdapAuthenticationServerTest extends ExternalLdapAuthenticationServerTestBase {

  private static ExternalLdapAuthenticationServerTest testServer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Security.AUTH_SERVER_BIND_ADDRESS, InetAddress.getLoopbackAddress().getHostName());
    cConf.set(Constants.Security.SSL.EXTERNAL_ENABLED, "false");
    cConf.setInt(Constants.Security.AUTH_SERVER_BIND_PORT, 0);

    configuration = cConf;
    sConfiguration = SConfiguration.create();

    ldapListenerConfig = InMemoryListenerConfig.createLDAPConfig("LDAP",
                                                                 InetAddress.getLoopbackAddress(),
                                                                 ldapPort, null);
    testServer = new ExternalLdapAuthenticationServerTest();
    testServer.setup();
  }


  @AfterClass
  public static void afterClass() throws Exception {
    testServer.tearDown();
  }

  @Override
  protected String getProtocol() {
    return "http";
  }

  @Override
  protected Map<String, String> getAuthRequestHeader() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Basic YWRtaW46cmVhbHRpbWU=");
    return headers;
  }

  @Override
  protected String getAuthenticatedUserName() {
   return "admin";
  }

   /**
   * Test request to server with empty password
   */
   @Test
   public void testEmptyPassword() throws Exception {
     HttpURLConnection urlConn = openConnection(getUrl(GrantAccessToken.Paths.GET_TOKEN));
     try {
       // base64 encoding of admin: (username=admin, password=empty string)
       urlConn.addRequestProperty("Authorization", "Basic YWRtaW46");
       Assert.assertEquals(401, urlConn.getResponseCode());
      } finally {
       urlConn.disconnect();
      }
    }
}
