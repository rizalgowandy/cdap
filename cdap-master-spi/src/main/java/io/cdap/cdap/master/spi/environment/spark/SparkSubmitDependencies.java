/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.environment.spark;

import java.util.Map;

/**
 * Represents spark submit dependencies files such as, jars, archives, files needed for submitting spark job.
 */
public class SparkSubmitDependencies {
  private final Map<String, String> filePaths;

  public SparkSubmitDependencies(Map<String, String> filePaths) {
    this.filePaths = filePaths;
  }

  public Map<String, String> getFilePaths() {
    return filePaths;
  }
}
