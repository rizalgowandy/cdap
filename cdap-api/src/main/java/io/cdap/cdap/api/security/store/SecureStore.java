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
import java.util.List;

/**
 * Provides read access to the secure store. For write access use {@link SecureStoreManager}.
 */
@Beta
public interface SecureStore {

  /**
   * List of metadata stored in the secure store.
   *
   * @param namespace The namespace that this key belongs to
   * @return A list of {@link SecureStoreMetadata} objects representing the data stored in the store
   * @throws IOException If there was a problem reading from the keystore
   * @throws Exception If the specified namespace does not exist
   */
  List<SecureStoreMetadata> list(String namespace) throws Exception;

  /**
   * Returns the secret and its metadata for the given namespace and name.
   * Use {@link #getData(String, String)} if the metadata is not required.
   *
   * @param namespace The namespace that this key belongs to
   * @param name Name of the data element
   * @return An object representing the securely stored data associated with the name
   * @throws IOException If there was a problem reading from the store
   * @throws Exception if the specified namespace or name does not exist
   */
  SecureStoreData get(String namespace, String name) throws Exception;

  /**
   * Returns the metadata for the secret.
   *
   * @param namespace The namespace that this key belongs to
   * @param name Name of the data element
   * @return Metadata for the securely stored data associated with the name
   * @throws IOException If there was a problem reading from the store
   * @throws Exception if the specified namespace or name does not exist
   */
  default SecureStoreMetadata getMetadata(String namespace, String name) throws Exception {
    return get(namespace, name).getMetadata();
  }

  /**
   * Returns the secret data.
   *
   * @param namespace The namespace that this key belongs to
   * @param name Name of the data element
   * @return The securely stored data associated with the name
   * @throws IOException If there was a problem reading from the store
   * @throws Exception if the specified namespace or name does not exist
   */
  default byte[] getData(String namespace, String name) throws Exception {
    return get(namespace, name).get();
  }
}
