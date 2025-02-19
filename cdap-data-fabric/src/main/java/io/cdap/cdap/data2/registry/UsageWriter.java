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

package io.cdap.cdap.data2.registry;

import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.ProgramId;

/**
 * Store program -> dataset/stream usage information. Differs from UsageRegistry in that
 * UsageRegistry does not have a remote implementation, usable from program runtime.
 */
public interface UsageWriter {

  /**
   * Registers usage of a dataset by multiple ids.
   *
   * @param users the users of the dataset
   * @param datasetId the dataset
   */
  default void registerAll(Iterable<? extends EntityId> users, DatasetId datasetId) {
    for (EntityId user : users) {
      register(user, datasetId);
    }
  }

  /**
   * Registers usage of a dataset by multiple ids.
   *
   * @param user the user of the dataset
   * @param datasetId the dataset
   */
  void register(EntityId user, DatasetId datasetId);

  /**
   * Registers usage of a dataset by a program.
   *
   * @param programId program
   * @param datasetInstanceId dataset
   */
  void register(ProgramId programId, DatasetId datasetInstanceId);
}
