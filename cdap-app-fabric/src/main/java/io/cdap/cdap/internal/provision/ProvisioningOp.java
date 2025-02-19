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

package io.cdap.cdap.internal.provision;

/**
 * Provisioning operations.
 */
public class ProvisioningOp {

  private final Type type;
  private final Status status;

  public ProvisioningOp(Type type, Status status) {
    this.type = type;
    this.status = status;
  }

  public Type getType() {
    return type;
  }

  public Status getStatus() {
    return status;
  }

  /**
   * Status of a provisioner operation.
   */
  public enum Status {
    REQUESTING_CREATE,
    POLLING_CREATE,
    INITIALIZING,
    CREATED,
    REQUESTING_DELETE,
    POLLING_DELETE,
    DELETED,
    FAILED,
    ORPHANED,
    CANCELLED
  }

  /**
   * Type of operation
   */
  public enum Type {
    PROVISION,
    DEPROVISION
  }
}
