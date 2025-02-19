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

package io.cdap.cdap.proto.metadata.lineage;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.proto.id.DatasetId;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a record in a {@link FieldLineageSummary}. Each record consists of dataset and subset
 * of its fields. The combination of both dataset and field can represent either the origin or
 * destination in the field lineage summary for a given field.
 */
@Beta
public class DatasetField {

  private final DatasetId dataset;
  private final Set<String> fields;

  public DatasetField(DatasetId dataset, Set<String> fields) {
    this.dataset = dataset;
    this.fields = Collections.unmodifiableSet(new HashSet<>(fields));
  }

  public DatasetId getDataset() {
    return dataset;
  }

  public Set<String> getFields() {
    return fields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetField that = (DatasetField) o;
    return Objects.equals(dataset, that.dataset)
        && Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataset, fields);
  }
}
