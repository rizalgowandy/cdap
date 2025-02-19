/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.logging.gateway.handlers;

import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.gateway.handlers.store.ProgramStore;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;

/**
 * Fetch {@link RunRecordDetail} directly from local {@link ProgramStore}
 */
public class LocalProgramRunRecordFetcher implements ProgramRunRecordFetcher {

  private final ProgramStore programStore;

  @Inject
  LocalProgramRunRecordFetcher(ProgramStore programStore) {
    this.programStore = programStore;
  }

  /**
   * Get {@link RunRecordDetail} for the given {@link ProgramRunId} Default to get with run
   * reference without version info
   *
   * @param runId identifies the program run to get {@link RunRecordDetail}
   * @return {@link RunRecordDetail}
   * @throws NotFoundException if the given {@link ProgramRunId} is not found
   */
  @Override
  public RunRecordDetail getRunRecordMeta(ProgramRunId runId) throws NotFoundException {
    return getRunRecordMeta(runId.getParent().getProgramReference(), runId.getVersion());
  }

  /**
   * Return {@link RunRecordDetail} for the given {@link ProgramReference} and run id
   *
   * @param programRef for which to fetch {@link RunRecordDetail}
   * @param runId for which to fetch {@link RunRecordDetail}
   * @return {@link RunRecordDetail}
   * @throws NotFoundException if the given {@link ProgramRunId} is not found
   */
  @Override
  public RunRecordDetail getRunRecordMeta(ProgramReference programRef, String runId)
      throws NotFoundException {
    RunRecordDetail runRecord = programStore.getRun(programRef, runId);
    if (runRecord == null) {
      throw new NotFoundException(
          String.format("No run record found for program %s and runID: %s", programRef, runId));
    }
    return runRecord;
  }
}
