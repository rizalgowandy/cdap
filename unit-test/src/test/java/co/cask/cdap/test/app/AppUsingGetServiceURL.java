/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.AbstractServiceWorker;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * AppWithServices with a DummyService, which other programs will hit via their context's getServiceURL method.
 */
public class AppUsingGetServiceURL extends AbstractApplication {
  public static final String APP_NAME = "AppUsingGetServiceURL";
  public static final String CENTRAL_SERVICE = "CentralService";
  public static final String SERVICE_WITH_WORKER = "ServiceWithWorker";
  public static final String PROCEDURE = "ForwardingProcedure";
  public static final String ANSWER = "MagicalString";
  private static final CountDownLatch countDownLatch = new CountDownLatch(1);

  public static CountDownLatch getCountDownLatch() {
    return countDownLatch;
  }

  @Override
  public void configure() {
      setName(APP_NAME);
      addProcedure(new ForwardingProcedure());
      addService(new CentralService());
      addService(new ServiceWithWorker());
   }


  public static final class ForwardingProcedure extends AbstractProcedure {

    @Handle("ping")
    public void ping(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      // Discover the CatalogLookup service via discovery service
      URL serviceURL = getContext().getServiceURL(CENTRAL_SERVICE);
      if (serviceURL == null) {
        responder.error(ProcedureResponse.Code.NOT_FOUND, "serviceURL is null");
        return;
      }
      String response = null;
      URL url = new URL(serviceURL, "ping");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
        try {
          response = new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
        } finally {
          conn.disconnect();
        }
      }

      if (response == null) {
        responder.error(ProcedureResponse.Code.FAILURE, "Failed to retrieve a response from the service");
      } else {
        responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), response);
      }
    }
  }

  /**
   * A service whose sole purpose is to check the ability for its worker to hit another service (via getServiceURL).
   */
  private static final class ServiceWithWorker extends AbstractService {

    @Override
    protected void configure() {
      setName(SERVICE_WITH_WORKER);
      addHandler(new NoOpHandler());
      addWorker(new PingingWorker());
    }
    public static final class NoOpHandler extends AbstractHttpServiceHandler {
      // handles nothing.
    }

    private static final class PingingWorker extends AbstractServiceWorker {

      @Override
      public void stop() {
        // no-op
      }

      @Override
      public void destroy() {
        // no-op
      }

      @Override
      public void run() {
        URL baseURL = getContext().getServiceURL(CENTRAL_SERVICE);
        if (baseURL == null) {
          return;
        }

        URL url = null;
        String response = null;
        try {
          url = new URL(baseURL, "ping");
        } catch (MalformedURLException e) {
          return;
        }

        try {
          HttpURLConnection conn = (HttpURLConnection) url.openConnection();
          if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
            try {
              response = new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
            } finally {
              conn.disconnect();
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
          return;
        }

        // The CentralService was pinged and the expected response was retrieved, so notify the test.
        if (ANSWER.equals(response)) {
          countDownLatch.countDown();
        }
      }
    }

  }


  /**
   * The central service which other programs will ping via their context's getServiceURL method.
   */
  private static final class CentralService extends AbstractService {

    @Override
    protected void configure() {
      setName("CentralService");
      addHandler(new NoOpHandler());
    }

    public static final class NoOpHandler extends AbstractHttpServiceHandler {
      @Path("/ping")
      @GET
      public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendString(200, ANSWER, Charsets.UTF_8);
      }
    }
  }
}
