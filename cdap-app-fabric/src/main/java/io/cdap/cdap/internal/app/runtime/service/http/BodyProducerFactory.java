/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.service.http;

import io.cdap.cdap.api.service.http.HttpContentProducer;
import io.cdap.http.BodyProducer;

/**
 * Interface for creating {@link BodyProducer} instance from {@link HttpContentProducer}. Instance
 * of this class is used in {@link DelayedHttpServiceResponder#execute(boolean)} to create {@link
 * BodyProducer} from {@link HttpContentProducer}.
 */
interface BodyProducerFactory {

  /**
   * Creates a {@link BodyProducer} from the given {@link HttpContentProducer}.
   *
   * @param contentProducer the content producer to delegate to
   * @param taskExecutor the {@link ServiceTaskExecutor} for executing user code
   */
  BodyProducer create(HttpContentProducer contentProducer, ServiceTaskExecutor taskExecutor);
}
