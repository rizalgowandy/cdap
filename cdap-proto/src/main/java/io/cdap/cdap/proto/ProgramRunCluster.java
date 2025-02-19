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
 *
 */

package io.cdap.cdap.proto;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Information about the cluster used for a program run.
 */
public class ProgramRunCluster {

  private final ProgramRunClusterStatus status;
  private final Long end;
  private final Integer numNodes;

  public ProgramRunCluster(ProgramRunClusterStatus status, @Nullable Long endTs,
      @Nullable Integer numNodes) {
    this.status = status;
    this.end = endTs;
    this.numNodes = numNodes;
  }

  public ProgramRunClusterStatus getStatus() {
    return status;
  }

  /**
   * @return timestamp in seconds when the cluster was deprovisioned or orphaned, or null if it is
   *     not in an end state
   */
  @Nullable
  public Long getEnd() {
    return end;
  }

  /**
   * @return number of nodes in the cluster. Can be null if the cluster has not been provisioned
   *     yet, or if it is on the local cluster.
   */
  @Nullable
  public Integer getNumNodes() {
    return numNodes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProgramRunCluster that = (ProgramRunCluster) o;

    return Objects.equals(status, that.status)
        && Objects.equals(end, that.end)
        && Objects.equals(numNodes, that.numNodes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, end, numNodes);
  }

  @Override
  public String toString() {
    return "ProgramRunCluster{"
        + "status=" + status
        + ", end=" + end
        + ", numNodes=" + numNodes
        + '}';
  }
}
