/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import com.google.inject.Inject;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.entity.EntityExistenceVerifier;
import io.cdap.cdap.proto.id.ProgramRunId;

/**
 * {@link EntityExistenceVerifier} for {@link ProgramRunId program runs}.
 */
public class ProgramRunExistenceVerifier implements EntityExistenceVerifier<ProgramRunId> {

  private final Store store;

  @Inject
  ProgramRunExistenceVerifier(Store store) {
    this.store = store;
  }

  @Override
  public void ensureExists(ProgramRunId runId) throws NotFoundException {
    if (store.getRun(runId) == null) {
      throw new NotFoundException(runId);
    }
  }
}
