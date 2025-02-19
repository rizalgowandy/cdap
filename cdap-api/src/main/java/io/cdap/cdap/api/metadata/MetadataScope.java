/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.metadata;

import io.cdap.cdap.api.annotation.Beta;
import java.util.EnumSet;
import java.util.Set;

/**
 * Represents the scope of metadata for a metadata entity.
 */
@Beta
public enum MetadataScope {
  USER,
  SYSTEM;

  public static final Set<MetadataScope> ALL = EnumSet.allOf(MetadataScope.class);
  public static final Set<MetadataScope> NONE = EnumSet.noneOf(MetadataScope.class);
}
