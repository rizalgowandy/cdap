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

package io.cdap.cdap.proto.credential;

import java.io.IOException;

/**
 * Provides a credential based on a profile and identity associated with the namespace.
 */
public interface NamespaceCredentialProvider {

  /**
   * Provisions a short-lived credential for the provided identity using the provided identity.
   *
   * @param namespace The identity namespace.
   * @param scopes    A comma separated list of OAuth scopes requested.
   * @return A short-lived credential.
   * @throws CredentialProvisioningException If provisioning the credential fails.
   * @throws IOException                     If any transport errors occur.
   * @throws NotFoundException               If the profile or identity are not found.
   */
  ProvisionedCredential provision(String namespace, String scopes)
      throws CredentialProvisioningException, IOException, NotFoundException;

  /**
   * Validates the provided identity.
   *
   * @param namespace      The identity namespace.
   * @param serviceAccount The service account to validate.
   * @throws IdentityValidationException If validation fails.
   * @throws IOException                 If any transport errors occur.
   */
  void validateIdentity(String namespace, String serviceAccount)
      throws IdentityValidationException, IOException;
}
