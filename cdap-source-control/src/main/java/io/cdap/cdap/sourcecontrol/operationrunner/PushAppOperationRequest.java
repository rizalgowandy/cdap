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

import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.CommitMeta;

/**
 * Information required by {@link SourceControlOperationRunner} to execute the task of pushing an application to
 * linked repository.
 */
public class PushAppOperationRequest {
  private final NamespaceId namespace;
  private final RepositoryConfig repoConfig;
  private final ApplicationDetail app;
  private final CommitMeta commitDetails;

  public PushAppOperationRequest(NamespaceId namespace,
                                 RepositoryConfig repoConfig,
                                 ApplicationDetail app,
                                 CommitMeta commitDetails) {
    this.namespace = namespace;
    this.repoConfig = repoConfig;
    this.app = app;
    this.commitDetails = commitDetails;
  }

  public NamespaceId getNamespaceId() {
    return namespace;
  }

  public RepositoryConfig getRepositoryConfig() {
    return repoConfig;
  }

  public ApplicationDetail getApp() {
    return app;
  }

  public CommitMeta getCommitDetails() {
    return commitDetails;
  }
}
