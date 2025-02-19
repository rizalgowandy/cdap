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

package io.cdap.cdap.etl.api.lineage.field;

import java.util.List;

/**
 * Interface for recording field lineage information from plugins.
 */
public interface LineageRecorder {

  /**
   * Record specified operations for the lineage purpose. This method can be called multiple times
   * from plugins. Operations provided during each call will be accumulated in a collection for the
   * lineage. This collection of all operations provided across multiple calls must have unique
   * names.
   *
   * @param fieldOperations the operations to be recorded
   */
  void record(List<FieldOperation> fieldOperations);
}
