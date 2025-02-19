/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.internal.asm;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.objectweb.asm.commons.Method;

/**
 * Util class containing helper functions to interact with ASM {@link Method}.
 */
public final class Methods {

  // Method descriptor of the LambdaMetafactory.metafactory method. It is for invokeDynamic lambda call.
  public static final String LAMBDA_META_FACTORY_METHOD_DESC =
      MethodType.methodType(CallSite.class, MethodHandles.Lookup.class, String.class,
          MethodType.class,
          MethodType.class, MethodHandle.class, MethodType.class).toMethodDescriptorString();

  public static Method getMethod(Class<?> returnType, String name, Class<?>... args) {
    return new Method(name, MethodType.methodType(returnType, args).toMethodDescriptorString());
  }

  private Methods() {
  }
}
