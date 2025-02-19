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

package io.cdap.cdap.data2.metadata.writer;

import io.cdap.cdap.proto.id.EntityId;

/**
 * A metadata publisher that does nothing.
 *
 * Note: this class is cannot be in test scope, because it is used in places likes JobQueueDebugge.
 */
public class NoOpMetadataPublisher implements MetadataPublisher {

  @Override
  public void publish(EntityId publisher, MetadataOperation metadataOperation) {
    // nop-op
  }

}
