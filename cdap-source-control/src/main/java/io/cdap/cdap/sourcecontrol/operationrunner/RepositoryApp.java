/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol.operationrunner;

import java.util.Objects;

/**
 * Response class encapsulating information for a single applications found in repository
 */
public class RepositoryApp {
  private final String name;
  private final String fileHash;

  public RepositoryApp(String name, String fileHash) {
    this.name = name;
    this.fileHash = fileHash;
  }

  public String getName() {
    return name;
  }

  public String getFileHash() {
    return fileHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RepositoryApp that = (RepositoryApp) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(fileHash, that.fileHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, fileHash);
  }
}
