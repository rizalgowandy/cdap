/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.streaming;

/**
 * Interface for handling Streaming batch events.
 */
public interface StreamingEventHandler {

  /**
   * Call on each completed batch.
   *
   * @param streamingContext @link{StreamingContext} Context object for this stage
   */
  void onBatchCompleted(StreamingContext streamingContext);

  /**
   * Call before starting each batch.
   *
   * @param streamingContext @link{StreamingContext} Context object for this stage
   */
  default void onBatchStarted(StreamingContext streamingContext) {
    // default implementation for backward compatibility
  }

  /**
   * Call before batch retry.
   *
   * @param streamingContext @link{StreamingContext} Context object for this stage
   */
  default void onBatchRetry(StreamingContext streamingContext) {
    // default implementation for backward compatibility
  }
}
