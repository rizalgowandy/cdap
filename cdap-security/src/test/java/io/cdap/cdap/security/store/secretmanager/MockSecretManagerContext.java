/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.security.store.secretmanager;

import io.cdap.cdap.securestore.spi.SecretManagerContext;
import io.cdap.cdap.securestore.spi.SecretNotFoundException;
import io.cdap.cdap.securestore.spi.SecretStore;
import io.cdap.cdap.securestore.spi.secret.Decoder;
import io.cdap.cdap.securestore.spi.secret.Encoder;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Mock secret manager context for unit tests.
 */
public class MockSecretManagerContext implements SecretManagerContext {
  @Override
  public Map<String, String> getProperties() {
    return Collections.emptyMap();
  }

  @Override
  public SecretStore getSecretStore() {
    return new SecretStore() {
      @Override
      public <T> T get(String namespace, String name, Decoder<T> decoder) throws SecretNotFoundException, IOException {
        // no-op
        return null;
      }

      @Override
      public <T> Collection<T> list(String namespace, Decoder<T> decoder) throws IOException {
        // no-op
        return null;
      }

      @Override
      public <T> void store(String namespace, String name, Encoder<T> encoder, T data) throws IOException {
        // no-op
      }

      @Override
      public void delete(String namespace, String name) throws SecretNotFoundException, IOException {
        // no-op
      }
    };
  }
}
