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

package io.cdap.cdap.proto;

import io.cdap.cdap.proto.id.ProgramReference;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Result for the program count, if there is an exception about the run count, the count will be
 * null, and the exception is contained
 */
public class RunCountResult {

  private final ProgramReference programReference;
  private final Long count;
  private final Exception exception;

  public RunCountResult(ProgramReference programReference, @Nullable Long count,
      @Nullable Exception exception) {
    this.programReference = programReference;
    this.count = count;
    this.exception = exception;
  }

  public ProgramReference getProgramReference() {
    return programReference;
  }

  @Nullable
  public Long getCount() {
    return count;
  }

  @Nullable
  public Exception getException() {
    return exception;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RunCountResult that = (RunCountResult) o;
    return Objects.equals(programReference, that.programReference)
        && Objects.equals(count, that.count)
        && Objects.equals(exception, that.exception);
  }

  @Override
  public int hashCode() {
    return Objects.hash(programReference, count, exception);
  }
}
