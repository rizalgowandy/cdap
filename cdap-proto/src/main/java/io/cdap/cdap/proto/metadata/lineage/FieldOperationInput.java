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
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.InputField;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents an input to a field operation: in case of a {@link ReadOperation}, a list of datasets
 * ({@link EndPoint}); otherwise a list of {@link InputField}.
 */
@Beta
public class FieldOperationInput {

  private final EndPoint endPoint;
  private final List<InputField> fields;

  private FieldOperationInput(@Nullable EndPoint endPoint, @Nullable List<InputField> fields) {
    this.endPoint = endPoint;
    this.fields = fields == null ? null : Collections.unmodifiableList(new ArrayList<>(fields));
  }

  /**
   * Create an instance of {@link FieldOperationInput} from a given EndPoint.
   *
   * @param endPoint an EndPoint representing input to the operation
   * @return instance of {@link FieldOperationInput}
   */
  public static FieldOperationInput of(EndPoint endPoint) {
    return new FieldOperationInput(endPoint, null);
  }

  /**
   * Create an instance of {@link FieldOperationInput} from a given list of InputFields.
   *
   * @param fields the list of InputField which represents an input to the operation
   * @return instance of {@link FieldOperationInput}
   */
  public static FieldOperationInput of(List<InputField> fields) {
    return new FieldOperationInput(null, fields);
  }

  @Nullable
  public EndPoint getEndPoint() {
    return endPoint;
  }

  @Nullable
  public List<InputField> getFields() {
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
    FieldOperationInput input = (FieldOperationInput) o;
    return Objects.equals(endPoint, input.endPoint)
        && Objects.equals(fields, input.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(endPoint, fields);
  }
}
