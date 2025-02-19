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
 * Provides a credential based on a profile and identity.
 */
public interface CredentialProvider {

  /**
   * Provisions a short-lived credential for the provided identity using the provided identity.
   *
   * @param namespace    The identity namespace.
   * @param identityName The identity name.
   * @param context      The context to use for provisioning.
   * @return A short-lived credential.
   * @throws CredentialProvisioningException If provisioning the credential fails.
   * @throws IOException                     If any transport errors occur.
   * @throws NotFoundException               If the profile or identity are not found.
   */
  ProvisionedCredential provision(String namespace, String identityName,
      CredentialProvisionContext context)
      throws CredentialProvisioningException, IOException, NotFoundException;

  /**
   * Validates the provided identity.
   *
   * @param namespace The identity namespace.
   * @param identity  The identity to validate.
   * @param context   The context to use for provisioning.
   * @throws IdentityValidationException If validation fails.
   * @throws IOException                 If any transport errors occur.
   * @throws NotFoundException           If the profile is not found.
   */
  void validateIdentity(String namespace, CredentialIdentity identity,
      CredentialProvisionContext context)
      throws IdentityValidationException, IOException, NotFoundException;
}
