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
package io.cdap.cdap.common.lang;

import com.google.common.reflect.TypeToken;
import io.cdap.cdap.internal.lang.Reflections;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ReflectionsTest {

  @Test
  public void testResolved() throws Exception {
    Assert.assertTrue(Reflections.isResolved(String.class));
    Assert.assertTrue(Reflections.isResolved(new TypeToken<Map<String, Set<Integer>>>() { }.getType()));

    TypeToken<Record<Set<Integer>>> typeToken = new TypeToken<Record<Set<Integer>>>() { };
    Type arrayType = Record.class.getMethod("getArray").getGenericReturnType();
    Assert.assertFalse(Reflections.isResolved(arrayType));
    Assert.assertTrue(Reflections.isResolved(typeToken.resolveType(arrayType).getType()));
  }

  private interface Record<T> {

    T get();

    T[] getArray();
  }
}
