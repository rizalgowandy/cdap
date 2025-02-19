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

package io.cdap.cdap.spi.data.table.field;

import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class FieldValidatorTest {
  private static final StructuredTableId SIMPLE_TABLE = new StructuredTableId("simpleTable");
  private static final String KEY = "key";
  private static final String KEY2 = "key2";
  private static final String KEY3 = "key3";
  private static final String STRING_COL = "col1";

  private static StructuredTableSchema schema;

  @BeforeClass
  public static void init() {
    StructuredTableSpecification spec = new StructuredTableSpecification.Builder()
      .withId(SIMPLE_TABLE)
      .withFields(Fields.intType(KEY), Fields.longType(KEY2), Fields.stringType(KEY3), Fields.stringType(STRING_COL))
      .withPrimaryKeys(KEY, KEY2, KEY3)
      .build();
    schema = new StructuredTableSchema(spec);
  }

  @Test
  public void testValidatePrimaryKeys() {
    FieldValidator validator = new FieldValidator(schema);

    // Success case
    validator.validatePrimaryKeys(Arrays.asList(Fields.intField(KEY, 10), Fields.longField(KEY2, 100L),
                                                Fields.stringField(KEY3, "s")), false);

    // Success case with prefix
    validator.validatePrimaryKeys(Arrays.asList(Fields.intField(KEY, 10), Fields.longField(KEY2, 100L)),
                                  true);
    validator.validatePrimaryKeys(Collections.singletonList(Fields.intField(KEY, 10)), true);

    // Test invalid type
    try {
      validator.validatePrimaryKeys(Arrays.asList(Fields.floatField(KEY, 10.0f), Fields.longField(KEY2, 100L),
                                                  Fields.stringField(KEY3, "s")), false);
      Assert.fail("Expected InvalidFieldException");
    } catch (InvalidFieldException e) {
      // expected
    }

    // Test invalid number
    try {
      validator.validatePrimaryKeys(Arrays.asList(Fields.intField(KEY, 10), Fields.longField(KEY2, 100L)),
                                    false);
      Assert.fail("Expected InvalidFieldException");
    } catch (InvalidFieldException e) {
      // expected
    }
  }

  @Test
  public void testValidateScanRange() {
    FieldValidator validator = new FieldValidator(schema);

    // Success case
    validator.validateScanRange(Range.all());
    validator.validateScanRange(Range.create(Arrays.asList(Fields.intField(KEY, 10),
                                                           Fields.longField(KEY2, 110L)),
                                             Range.Bound.INCLUSIVE,
                                             Arrays.asList(Fields.intField(KEY, 20),
                                                           Fields.longField(KEY2, 100L)),
                                             Range.Bound.EXCLUSIVE));

    validator.validateScanRange(Range.create(Collections.singletonList(Fields.intField(KEY, 10)),
                                             Range.Bound.INCLUSIVE,
                                             Collections.singletonList(Fields.intField(KEY, 20)),
                                             Range.Bound.INCLUSIVE));

    // Test invalid range: no primary keys
    try {
      validator.validateScanRange(Range.create(Collections.singletonList(Fields.stringField(STRING_COL, "110L")),
                                               Range.Bound.INCLUSIVE,
                                               Collections.singletonList(Fields.stringField(STRING_COL, "120L")),
                                               Range.Bound.INCLUSIVE));
      Assert.fail("Expected InvalidFieldException because of not staring with a primary key");
    } catch (InvalidFieldException e) {
      // expected
    }

    // Test invalid range: no primary keys
    try {
      validator.validateScanRange(Range.create(Collections.singletonList(Fields.longField(KEY2, 110L)),
                                               Range.Bound.INCLUSIVE,
                                               null,
                                               Range.Bound.INCLUSIVE));
      Assert.fail("Expected InvalidFieldException because of not staring with a prefixed key");
    } catch (InvalidFieldException e) {
      // expected
    }

    // Test invalid field
    try {
      validator.validateScanRange(Range.create(Collections.singletonList(Fields.intField("NONEXISTKEY", 110)),
                                               Range.Bound.INCLUSIVE,
                                               Collections.singletonList(Fields.longField(KEY2, 100L)),
                                               Range.Bound.INCLUSIVE));
      Assert.fail("Expected InvalidFieldException because of invalid field");
    } catch (InvalidFieldException e) {
      // expected
    }
  }

  @Test
  public void testValidatePartialPrimaryKeys() {
    FieldValidator validator = new FieldValidator(schema);

    // Success case
    validator.validatePartialPrimaryKeys(Arrays.asList(Fields.intField(KEY, 10),
                                                       Fields.longField(KEY2, 100L),
                                                       Fields.stringField(KEY3, "s")));

    // Success case with prefix
    validator.validatePartialPrimaryKeys(Arrays.asList(Fields.intField(KEY, 10),
                                                       Fields.longField(KEY2, 100L)));

    // Success case with partial keys
    validator.validatePartialPrimaryKeys(Arrays.asList(Fields.intField(KEY2, 10),
                                                       Fields.stringField(KEY3, "s")));

    // Test invalid type
    try {
      validator.validatePartialPrimaryKeys(Arrays.asList(Fields.stringField(KEY3, "s"),
                                                         Fields.floatField(KEY, 10.0f),
                                                         Fields.longField(KEY2, 100L)));
      Assert.fail("Expected InvalidFieldException");
    } catch (InvalidFieldException e) {
      // expected
    }

    // Test invalid number
    try {
      validator.validatePartialPrimaryKeys(Arrays.asList(Fields.intField(KEY, 10),
                                                         Fields.longField(KEY2, 100L),
                                                         Fields.longField("notExitField", 100L),
                                                         Fields.longField(KEY2, 100L)));
      Assert.fail("Expected InvalidFieldException");
    } catch (InvalidFieldException e) {
      // expected
    }
  }
}
