/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.proto.metadata.lineage;

import io.cdap.cdap.proto.id.ProgramId;
import java.util.Objects;

/**
 * Represents the program information including when it was last executed.
 */
public class ProgramInfo {

  private final ProgramId program;
  private final long lastExecutedTimeInSeconds;

  public ProgramInfo(ProgramId program, long lastExecutedTimeInSeconds) {
    this.program = program;
    this.lastExecutedTimeInSeconds = lastExecutedTimeInSeconds;
  }

  public ProgramId getProgram() {
    return program;
  }

  public long getLastExecutedTimeInSeconds() {
    return lastExecutedTimeInSeconds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProgramInfo that = (ProgramInfo) o;
    return lastExecutedTimeInSeconds == that.lastExecutedTimeInSeconds
        && Objects.equals(program, that.program);
  }

  @Override
  public int hashCode() {
    return Objects.hash(program, lastExecutedTimeInSeconds);
  }
}
