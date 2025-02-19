/*
 * Copyright © 2022 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.error.api;

import java.util.Set;

/**
 * Interface that an exception that wants to add error tags needs to implement. There are two ways
 * this can be used. 1) An exception may directly implement it. 2) An exception may be wrapped by
 * another exception that implements it
 */
public interface ErrorTagProvider {

  enum ErrorTag {
    SYSTEM("system"),
    USER("user"),
    DEPENDENCY("dependency"),
    PLUGIN("plugin"),
    CONFIGURATION("configuration");

    private final String displayName;

    ErrorTag(String name) {
      displayName = name;
    }

    @Override
    public String toString() {
      return displayName;
    }

  }

  ;

  Set<ErrorTag> getErrorTags();

}
