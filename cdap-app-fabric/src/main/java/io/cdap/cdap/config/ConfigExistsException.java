/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.config;

import io.cdap.cdap.common.AlreadyExistsException;

/**
 * Thrown when a Configuration is present when it is not expected to be.
 */
public final class ConfigExistsException extends AlreadyExistsException {


  public ConfigExistsException(String namespace, String type, String id) {
    super(
        String.format("Configuration: %s of Type: %s in Namespace: %s is already present", id, type,
            namespace));
  }
}
