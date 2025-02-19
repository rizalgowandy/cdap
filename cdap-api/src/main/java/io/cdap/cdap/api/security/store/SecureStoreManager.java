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

package io.cdap.cdap.api.security.store;

import io.cdap.cdap.api.annotation.Beta;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides write access to the secure store. For read access use {@link SecureStore}.
 */
@Beta
public interface SecureStoreManager {

  /**
   * Stores an element in the secure store.
   *
   * @param namespace The namespace that this key belongs to
   * @param name This is the identifier that will be used to retrieve this element
   * @param data The sensitive data that has to be securely stored
   * @param description User provided description of the entry
   * @param properties associated with this element
   * @throws IOException If the attempt to store the element failed
   * @throws Exception If the specified namespace does not exist
   */
  void put(String namespace, String name, String data, @Nullable String description,
      Map<String, String> properties) throws Exception;

  /**
   * Deletes the element with the given name.
   *
   * @param namespace The namespace that this key belongs to
   * @param name of the element to delete
   * @throws IOException If the store is not initialized or if the key could not be removed
   * @throws Exception If the specified namespace or name does not exist
   */
  void delete(String namespace, String name) throws Exception;
}
