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

package io.cdap.cdap.sourcecontrol;

import io.cdap.cdap.common.NotFoundException;

/**
 * Exception thrown when no auth strategy was found for the given {@link
 * io.cdap.cdap.proto.sourcecontrol.Provider} and {@link io.cdap.cdap.proto.sourcecontrol.AuthType}.
 */
public class AuthenticationStrategyNotFoundException extends NotFoundException {

  public AuthenticationStrategyNotFoundException(String message) {
    super(message);
  }
}
