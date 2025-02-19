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

package io.cdap.cdap.common;

import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Thrown when a namespace repository configuration is not found in CDAP.
 */
public class RepositoryNotFoundException extends NotFoundException {

  private final NamespaceId namespace;

  /**
   * Constructor for {@code RepositoryNotFoundException}
   * @param id the namespace ID
   */
  public RepositoryNotFoundException(NamespaceId id) {
    super(String.format("There is no repository configuration for namespace %s. "
        + "Please configure it in Namespace Admin page.", id.getNamespace()));
    this.namespace = id;
  }

  public NamespaceId getNamespace() {
    return namespace;
  }
}
