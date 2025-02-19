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

package io.cdap.cdap;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.service.BasicService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.worker.AbstractWorker;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Test Application with services for the new application API.
 */
public class AppWithServices extends AbstractApplication {

  public static final String NAME = AppWithServices.class.getSimpleName();
  public static final String SERVICE_NAME = "NoOpService";

  /**
   * Override this method to configure the application.
   */
  @Override
  public void configure() {
    setName(NAME);
    setDescription("Application with Services");
    addService(new BasicService(SERVICE_NAME, new PingHandler(), new MultiPingHandler()));
    addWorker(new DummyWorker());
  }

  public static class DummyWorker extends AbstractWorker {
    @Override
    public void run() {
      // does nothing on purpose
    }
  }

  public static final class PingHandler extends AbstractHttpServiceHandler {

    @Path("ping")
    @GET
    public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }
  }

  @Path("/multi")
  public static final class MultiPingHandler extends AbstractHttpServiceHandler {

    // No method level path, multiple request types.
    @POST
    @GET
    public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }

    @GET
    @Path("/ping")
    public void pingHandler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }

    // this non-public "handler" method is here to test that it is not exposed in the service specification, since it is
    // not exposed as an endpoint on the http handler either.
    @GET
    @Path("/protected/ping")
    protected void protectedPingHandler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }
  }
}
