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

package io.cdap.cdap.etl.spark.streaming;

/**
 * Class for holding various retry settings
 */
public class StreamingRetrySettings {
  private final long maxRetryTimeInMins;
  private final long baseDelayInSeconds;
  private final long maxDelayInSeconds;

  public StreamingRetrySettings(long maxRetryTimeInMins, long baseDelayInSeconds, long maxDelayInSeconds) {
    this.maxRetryTimeInMins = maxRetryTimeInMins;
    this.baseDelayInSeconds = baseDelayInSeconds;
    this.maxDelayInSeconds = maxDelayInSeconds;
  }

  public long getMaxRetryTimeInMins() {
    return maxRetryTimeInMins;
  }

  public long getBaseDelayInSeconds() {
    return baseDelayInSeconds;
  }

  public long getMaxDelayInSeconds() {
    return maxDelayInSeconds;
  }
}
