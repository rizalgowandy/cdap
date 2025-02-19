/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.nosql;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.data2.dataset2.lib.table.MDSKey;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The nosql structured row represents a row in the nosql table.
 */
public final class NoSqlStructuredRow implements StructuredRow {

  private final Row row;
  private final StructuredTableSchema tableSchema;
  private final Map<String, Object> keyFields;
  private final Collection<Field<?>> keys;

  NoSqlStructuredRow(Row row, StructuredTableSchema tableSchema) {
    this.row = row;
    this.tableSchema = tableSchema;
    this.keys = new ArrayList<>();
    this.keyFields = extractKeys();
  }

  @Nullable
  @Override
  public Integer getInteger(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Nullable
  @Override
  public Long getLong(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Nullable
  @Override
  public Boolean getBoolean(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Nullable
  @Override
  public String getString(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Nullable
  @Override
  public Float getFloat(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Nullable
  @Override
  public Double getDouble(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Nullable
  @Override
  public byte[] getBytes(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Override
  public Collection<Field<?>> getPrimaryKeys() {
    return keys;
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private <T> T get(String fieldName) throws InvalidFieldException {
    FieldType.Type expectedType = tableSchema.getType(fieldName);
    if (expectedType == null) {
      // Field is not present in the schema
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName);
    }

    // Check if field is a key
    if (tableSchema.isPrimaryKeyColumn(fieldName)) {
      return (T) keyFields.get(fieldName);
    }

    // Field is a regular column
    return getFieldValue(fieldName, expectedType);
  }

  private Map<String, Object> extractKeys() {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    MDSKey.Splitter splitter = new MDSKey(row.getRow()).split();
    // extract the first part since we always have the table name as the prefix
    splitter.getString();
    for (String key : tableSchema.getPrimaryKeys()) {
      // the NullPointerException should never be thrown since the primary keys must always have a type
      FieldType.Type type = tableSchema.getType(key);
      switch (Objects.requireNonNull(type)) {
        case INTEGER:
          int intVal = splitter.getInt();
          builder.put(key, intVal);
          keys.add(Fields.intField(key, intVal));
          break;
        case LONG:
          long longVal = splitter.getLong();
          builder.put(key, longVal);
          keys.add(Fields.longField(key, longVal));
          break;
        case STRING:
          String stringVal = splitter.getString();
          keys.add(Fields.stringField(key, stringVal));
          builder.put(key, stringVal);
          break;
        case BYTES:
          byte[] bytesVal = splitter.getBytes();
          keys.add(Fields.bytesField(key, bytesVal));
          builder.put(key, bytesVal);
          break;
        case BOOLEAN:
          boolean booleanVal = splitter.getBoolean();
          keys.add(Fields.booleanField(key, booleanVal));
          builder.put(key, booleanVal);
          break;
        default:
          // this should never happen since all the keys are from the table schema and should never contain other types
          throw new IllegalStateException(
              String.format("The type %s of the primary key %s is not a valid key type", type,
                  key));
      }
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private <T> T getFieldValue(String fieldName, FieldType.Type expectedType)
      throws InvalidFieldException {
    switch (expectedType) {
      case INTEGER:
        return (T) row.getInt(fieldName);
      case LONG:
        return (T) row.getLong(fieldName);
      case FLOAT:
        return (T) row.getFloat(fieldName);
      case DOUBLE:
        return (T) row.getDouble(fieldName);
      case STRING:
        return (T) row.getString(fieldName);
      case BYTES:
        return (T) row.get(fieldName);
      case BOOLEAN:
        return (T) row.getBoolean(fieldName);
      default:
        throw new InvalidFieldException(tableSchema.getTableId(), fieldName);
    }
  }
}
