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

package io.cdap.cdap.security.authorization;

import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.common.test.NoopAdmin;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationContext;
import io.cdap.cdap.security.store.DummySecureStoreService;
import java.util.Map;
import java.util.Properties;
import org.apache.tephra.TransactionFailureException;

/**
 * A no-op implementation of {@link AuthorizationContextFactory} for use in tests.
 */
public class NoOpAuthorizationContextFactory implements AuthorizationContextFactory {
  @Override
  public AuthorizationContext create(Properties extensionProperties) {
    return new DefaultAuthorizationContext(extensionProperties, new NoOpDatasetContext(), new NoopAdmin(),
                                           new NoOpTransactional(), new AuthenticationTestContext(),
                                           new DummySecureStoreService());
  }

  private static final class NoOpTransactional implements Transactional {
    @Override
    public void execute(TxRunnable runnable) throws TransactionFailureException {
      // no-op
    }

    @Override
    public void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {
      // no-op
    }
  }

  private static class NoOpDatasetContext implements DatasetContext {
    @Override
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
      throw new DatasetInstantiationException("NoOpDatasetContext cannot instantiate datasets");
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
      throw new DatasetInstantiationException("NoOpDatasetContext cannot instantiate datasets");
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
      throw new DatasetInstantiationException("NoOpDatasetContext cannot instantiate datasets");
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
      throw new DatasetInstantiationException("NoOpDatasetContext cannot instantiate datasets");
    }

    @Override
    public void releaseDataset(Dataset dataset) {
      // no-op
    }

    @Override
    public void discardDataset(Dataset dataset) {
      // no-op
    }
  }
}
