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

package io.cdap.cdap.spi.data.table.field;

import io.cdap.cdap.api.annotation.Beta;
import javax.annotation.Nullable;

/**
 * Convenience methods to work on {@link Field} and {@link FieldType}.
 */
@Beta
public final class Fields {

  private Fields() {
    // to prevent instantiation of the class
  }

  /**
   * @return the FieldType of INTEGER with the given name
   */
  public static FieldType intType(String name) {
    return new FieldType(name, FieldType.Type.INTEGER);
  }

  /**
   * @return the FieldType of LONG with the given name
   */
  public static FieldType longType(String name) {
    return new FieldType(name, FieldType.Type.LONG);
  }

  /**
   * @return the FieldType of STRING with the given name
   */
  public static FieldType stringType(String name) {
    return new FieldType(name, FieldType.Type.STRING);
  }

  /**
   * @return the FieldType of BOOLEAN with the given name
   */
  public static FieldType booleanType(String name) {
    return new FieldType(name, FieldType.Type.BOOLEAN);
  }

  /**
   * @return the FieldType of FLOAT with the given name
   */
  public static FieldType floatType(String name) {
    return new FieldType(name, FieldType.Type.FLOAT);
  }

  /**
   * @return the FieldType of DOUBLE with the given name
   */
  public static FieldType doubleType(String name) {
    return new FieldType(name, FieldType.Type.DOUBLE);
  }

  /**
   * @return the FieldType of BYTES with the given name
   */
  public static FieldType bytesType(String name) {
    return new FieldType(name, FieldType.Type.BYTES);
  }

  /**
   * Create a field with integer value.
   *
   * @param name name of the field
   * @param value value of the field
   * @return a field with type integer
   */
  public static Field<Integer> intField(String name, @Nullable Integer value) {
    return new Field<>(new FieldType(name, FieldType.Type.INTEGER), value);
  }

  /**
   * Create a field with long value.
   *
   * @param name name of the field
   * @param value value of the field
   * @return a field with type long
   */
  public static Field<Long> longField(String name, @Nullable Long value) {
    return new Field<>(new FieldType(name, FieldType.Type.LONG), value);
  }

  /**
   * Create a field with string value.
   *
   * @param name name of the field
   * @param value value of the field
   * @return a field with type string
   */
  public static Field<String> stringField(String name, @Nullable String value) {
    return new Field<>(new FieldType(name, FieldType.Type.STRING), value);
  }

  /**
   * Create a field with float value.
   *
   * @param name name of the field
   * @param value value of the field
   * @return a field with type float
   */
  public static Field<Float> floatField(String name, @Nullable Float value) {
    return new Field<>(new FieldType(name, FieldType.Type.FLOAT), value);
  }

  /**
   * Create a field with boolean value.
   *
   * @param name name of the field
   * @param value value of the field
   * @return a field with type boolean
   */
  public static Field<Boolean> booleanField(String name, @Nullable Boolean value) {
    return new Field<>(new FieldType(name, FieldType.Type.BOOLEAN), value);
  }

  /**
   * Create a field with double value.
   *
   * @param name name of the field
   * @param value value of the field
   * @return a field with type double
   */
  public static Field<Double> doubleField(String name, @Nullable Double value) {
    return new Field<>(new FieldType(name, FieldType.Type.DOUBLE), value);
  }

  /**
   * Create a field with byte[] value.
   *
   * @param name name of the field
   * @param value value of the field
   * @return a field with type BYTES
   */
  public static Field<byte[]> bytesField(String name, @Nullable byte[] value) {
    return new Field<>(new FieldType(name, FieldType.Type.BYTES), value);
  }

  /**
   * @return true if the type is allowed to be part of a primary key, false otherwise
   */
  public static boolean isPrimaryKeyType(FieldType.Type type) {
    return FieldType.PRIMARY_KEY_TYPES.contains(type);
  }

  /**
   * @return true if the type is allowed to be an index column, false otherwise
   */
  public static boolean isIndexColumnType(FieldType.Type type) {
    return FieldType.INDEX_COLUMN_TYPES.contains(type);
  }
}
