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

import io.cdap.cdap.proto.artifact.AppRequest;

/**
 * Encapsulates the information generated from pull operation.
 */
public class PullAppResponse<T> {

  private final String applicationName;
  private final String applicationFileHash;
  private final AppRequest<T> appRequest;
  private final String commitId;

  /**
   * Default contrictor for PullAppResponse.
   */
  public PullAppResponse(String applicationName, String applicationFileHash,
      AppRequest<T> appRequest, String commitId) {
    this.applicationName = applicationName;
    this.applicationFileHash = applicationFileHash;
    this.appRequest = appRequest;
    this.commitId = commitId;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getApplicationFileHash() {
    return applicationFileHash;
  }

  public AppRequest<?> getAppRequest() {
    return appRequest;
  }

  public String getCommitId() {
    return commitId;
  }
}
