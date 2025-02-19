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

package io.cdap.cdap.spi.data.table;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.spi.data.StructuredTable;
import java.util.Objects;

/**
 * Specifies the Id of a {@link StructuredTable}.
 */
@Beta
public class StructuredTableId {

  private final String name;

  /**
   * @param name table name, the name can only contain alpha-numeric characters and underscore.
   *     The name will always be lower case. The name has to start with an alphabet.
   */
  public StructuredTableId(String name) {
    this.name = name.toLowerCase();
  }

  /**
   * @return the table name
   */
  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StructuredTableId tableId = (StructuredTableId) o;
    return Objects.equals(name, tableId.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return "StructuredTableId{"
        + "name='" + name + '\''
        + '}';
  }
}
