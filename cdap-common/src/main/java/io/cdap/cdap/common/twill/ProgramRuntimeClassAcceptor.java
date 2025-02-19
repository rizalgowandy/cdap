/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common.twill;

import java.net.URL;
import org.apache.twill.api.ClassAcceptor;

/**
 * A {@link ClassAcceptor} for program execution.
 */
public class ProgramRuntimeClassAcceptor extends HadoopClassExcluder {

  @Override
  public boolean accept(String className, URL classUrl, URL classPathUrl) {
    if (!super.accept(className, classUrl, classPathUrl)) {
      return false;
    }
    // Exclude metadata search, in which brings in elastic search and lucene. They are not used by program runtime.
    return !className.startsWith("io.cdap.cdap.metadata.elastic.");
  }
}
