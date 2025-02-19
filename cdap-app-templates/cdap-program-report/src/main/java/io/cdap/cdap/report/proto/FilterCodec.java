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
package io.cdap.cdap.report.proto;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.report.util.ReportField;
import java.lang.reflect.Type;
import javax.annotation.Nullable;

/**
 * JSON codec for {@link Filter}
 */
public class FilterCodec implements JsonSerializer<Filter>, JsonDeserializer<Filter> {

  private static final Type INT_RANGE_FILTER_TYPE =
      new TypeToken<RangeFilter<Integer>>() {
      }.getType();
  private static final Type LONG_RANGE_FILTER_TYPE =
      new TypeToken<RangeFilter<Long>>() {
      }.getType();
  private static final Type STRING_VALUE_FILTER_TYPE =
      new TypeToken<ValueFilter<String>>() {
      }.getType();

  /**
   * Deserializes a JSON String as {@link Filter}. Determines the class and data type of the filter
   * according to the field name that the filter contains.
   */
  @Nullable
  @Override
  public Filter deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    if (json == null) {
      return null;
    }
    if (!(json instanceof JsonObject)) {
      throw new JsonParseException(
          "Expected a JsonObject but found a " + json.getClass().getName());
    }

    JsonObject object = (JsonObject) json;
    JsonElement fieldName = object.get("fieldName");
    if (fieldName == null) {
      throw new JsonParseException("Field name must be specified for filters");
    }
    ReportField field = ReportField.valueOfFieldName(fieldName.getAsString());
    if (field == null) {
      throw new JsonParseException(
          String.format("Invalid field name '%s'. Field name must be one of: [%s]", fieldName,
              String.join(", ", ReportField.FIELD_NAME_MAP.keySet())));
    }
    Filter filter = null;
    // if the object contains "range" field, try to deserialize it as a range filter
    if (object.get("range") != null) {
      // Use the type token that matches the class of this field's value to deserialize the JSON
      if (field.getValueClass().equals(Integer.class)) {
        filter = context.deserialize(json, INT_RANGE_FILTER_TYPE);
      } else if (field.getValueClass().equals(Long.class)) {
        filter = context.deserialize(json, LONG_RANGE_FILTER_TYPE);
      }
      // otherwise, try to deserialize it as a value filter
    } else if (field.getValueClass().equals(String.class)) {
      // Use the type token that matches the class of this field's value to deserialize the JSON
      filter = context.deserialize(json, STRING_VALUE_FILTER_TYPE);
    }
    if (filter == null) {
      // this should never happen. If the field's applicable filters contains value filter,
      // there must be a know class matches the class of its value
      throw new JsonParseException(
          String.format("No applicable filter found for field %s with value type %s.",
              fieldName, field.getValueClass().getName()));
    }
    return filter;
  }

  @Override
  public JsonElement serialize(Filter src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add("fieldName", new JsonPrimitive(src.getFieldName()));
    if (src instanceof RangeFilter) {
      jsonObject.add("range", context.serialize(((RangeFilter) src).getRange()));
    }
    if (src instanceof ValueFilter) {
      ValueFilter valueFilter = (ValueFilter) src;
      if (valueFilter.getWhitelist().size() > 0) {
        jsonObject.add("whitelist", context.serialize(valueFilter.getWhitelist()));
      }
      if (valueFilter.getBlacklist().size() > 0) {
        jsonObject.add("blacklist", context.serialize(valueFilter.getBlacklist()));
      }
    }
    return jsonObject;
  }
}
