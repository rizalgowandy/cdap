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

package io.cdap.cdap.gateway.handlers.meta;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.internal.remote.MethodArgument;
import io.cdap.http.AbstractHttpHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Implements common functionality for reading method arguments and deserialize them.
 */
class AbstractRemoteSystemOpsHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private static final Type METHOD_ARGUMENT_LIST_TYPE = new TypeToken<List<MethodArgument>>() {
  }.getType();

  // we don't share the same version as other handlers in app fabric, so we can upgrade/iterate faster
  protected static final String VERSION = "/v1";

  Iterator<MethodArgument> parseArguments(FullHttpRequest request) {
    String body = request.content().toString(StandardCharsets.UTF_8);
    List<MethodArgument> arguments = GSON.fromJson(body, METHOD_ARGUMENT_LIST_TYPE);
    return arguments.iterator();
  }

  @Nullable
  <T> T deserializeNext(Iterator<MethodArgument> arguments)
      throws ClassNotFoundException, BadRequestException {
    return deserializeNext(arguments, null);
  }

  @Nullable
  <T> T deserializeNext(Iterator<MethodArgument> arguments,
      @Nullable Type typeOfT) throws ClassNotFoundException, BadRequestException {
    if (!arguments.hasNext()) {
      throw new BadRequestException("Expected additional elements.");
    }

    MethodArgument argument = arguments.next();
    if (argument == null) {
      return null;
    }
    JsonElement value = argument.getValue();
    if (value == null) {
      return null;
    }
    if (typeOfT != null) {
      return GSON.fromJson(value, typeOfT);
    }
    return GSON.<T>fromJson(value, Class.forName(argument.getType()));
  }
}
