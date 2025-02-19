/*
 * Copyright © 2017 Cask Data, Inc.
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
package io.cdap.cdap.api.workflow;

import io.cdap.cdap.api.common.PropertyProvider;
import io.cdap.cdap.api.dataset.Dataset;
import java.util.Set;

/**
 * Specification for the condition that will be executed as a part of {@link Workflow}.
 */
public interface ConditionSpecification extends PropertyProvider {

  /**
   * @return Class name of the condition.
   */
  String getClassName();

  /**
   * @return Name of the condition.
   */
  String getName();

  /**
   * @return Description of the condition.
   */
  String getDescription();

  /**
   * @return an immutable set of {@link Dataset} name that are used by the {@link Condition}
   */
  Set<String> getDatasets();
}
