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
package io.cdap.cdap.internal.app.runtime.workflow;

import com.google.common.base.Supplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.customaction.CustomActionSpecification;
import io.cdap.cdap.api.workflow.ConditionSpecification;
import io.cdap.cdap.api.workflow.WorkflowActionNode;
import io.cdap.cdap.proto.codec.ConditionSpecificationCodec;
import io.cdap.cdap.proto.codec.CustomActionSpecificationCodec;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.lang.reflect.Type;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * A HttpHandler for handling Workflow REST API.
 */
public final class WorkflowServiceHandler extends AbstractHttpHandler {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(CustomActionSpecification.class,
          new CustomActionSpecificationCodec())
      .registerTypeAdapter(ConditionSpecification.class,
          new ConditionSpecificationCodec())
      .create();

  private final Supplier<List<WorkflowActionNode>> statusSupplier;

  WorkflowServiceHandler(Supplier<List<WorkflowActionNode>> statusSupplier) {
    this.statusSupplier = statusSupplier;
  }

  /**
   * Provides response to {@code /status} call to gives the latest status of this workflow.
   */
  @GET
  @Path("/status")
  public void handleStatus(HttpRequest request, HttpResponder responder) {
    Type type = new TypeToken<List<WorkflowActionNode>>() {
    }.getType();
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(statusSupplier.get(), type));
  }
}
