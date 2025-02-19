/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.format;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.format.Formats;
import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AvroRecordFormatTest {

  @Test
  public void testMultipleReads() throws Exception {
    Schema schema = Schema.recordOf("record", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    FormatSpecification formatSpecification =
      new FormatSpecification(Formats.AVRO, schema, Collections.emptyMap());

    org.apache.avro.Schema avroSchema = convertSchema(schema);
    RecordFormat<ByteBuffer, StructuredRecord> format = RecordFormats.createInitializedFormat(formatSpecification);

    GenericRecord record = new GenericRecordBuilder(avroSchema).set("x", 5).build();
    StructuredRecord actual = format.read(toByteBuffer(record));
    Assert.assertEquals(5, (int) actual.get("x"));

    record = new GenericRecordBuilder(avroSchema).set("x", 10).build();
    actual = format.read(toByteBuffer(record));
    Assert.assertEquals(10, (int) actual.get("x"));
  }

  @Test
  public void testFlatRecord() throws Exception {
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.INT))),
      Schema.Field.of("nullable", Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("nullable2", Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL)))
    );
    FormatSpecification formatSpecification = new FormatSpecification(
      Formats.AVRO,
      schema,
      Collections.emptyMap()
    );

    org.apache.avro.Schema avroSchema = convertSchema(schema);
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("int", Integer.MAX_VALUE)
      .set("long", Long.MAX_VALUE)
      .set("boolean", false)
      .set("bytes", Charsets.UTF_8.encode("hello world"))
      .set("double", Double.MAX_VALUE)
      .set("float", Float.MAX_VALUE)
      .set("string", "foo bar")
      .set("array", Lists.newArrayList(1, 2, 3))
      .set("map", ImmutableMap.of("k1", 1, "k2", 2))
      .set("nullable", null)
      .set("nullable2", "Hello")
      .build();

    RecordFormat<ByteBuffer, StructuredRecord> format = RecordFormats.createInitializedFormat(formatSpecification);

    StructuredRecord actual = format.read(toByteBuffer(record));
    Assert.assertEquals(Integer.MAX_VALUE, (int) actual.get("int"));
    Assert.assertEquals(Long.MAX_VALUE, (long) actual.get("long"));
    Assert.assertFalse(actual.get("boolean"));
    Assert.assertArrayEquals(Bytes.toBytes("hello world"), Bytes.toBytes((ByteBuffer) actual.get("bytes")));
    Assert.assertEquals(Double.MAX_VALUE, actual.get("double"), 0.0001d);
    Assert.assertEquals(Float.MAX_VALUE, actual.get("float"), 0.0001f);
    Assert.assertEquals("foo bar", actual.get("string"));
    Assert.assertEquals(Lists.newArrayList(1, 2, 3), actual.get("array"));
    assertMapEquals(ImmutableMap.of("k1", 1, "k2", 2), actual.get("map"));
    Assert.assertNull(actual.get("nullable"));
    Assert.assertEquals("Hello", actual.get("nullable2"));
  }

  @Test
  public void testNestedRecord() throws Exception {
    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))
    );
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("record", innerSchema));
    org.apache.avro.Schema avroInnerSchema = convertSchema(innerSchema);
    org.apache.avro.Schema avroSchema = convertSchema(schema);

    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("int", Integer.MAX_VALUE)
      .set("record",
           new GenericRecordBuilder(avroInnerSchema)
             .set("int", 5)
             .set("double", 3.14159)
             .set("array", ImmutableList.of(1.0f, 2.0f))
             .set("map", ImmutableMap.of("key", "value"))
             .build())
      .build();

    FormatSpecification formatSpecification = new FormatSpecification(
      Formats.AVRO,
      schema,
      Collections.emptyMap()
    );
    RecordFormat<ByteBuffer, StructuredRecord> format = RecordFormats.createInitializedFormat(formatSpecification);

    StructuredRecord actual = format.read(toByteBuffer(record));
    Assert.assertEquals(Integer.MAX_VALUE, (int) actual.get("int"));
    StructuredRecord actualInner = actual.get("record");
    Assert.assertEquals(5, (int) actualInner.get("int"));
    Assert.assertEquals(3.14159d, actualInner.get("double"), 0.0001d);

    List<Float> array = actualInner.get("array");
    Assert.assertEquals(ImmutableList.of(1.0f, 2.0f), array);

    Map<String, String> map = actualInner.get("map");
    Assert.assertEquals(ImmutableMap.of("key", "value"), map);
  }

  private org.apache.avro.Schema convertSchema(Schema cdapSchema) {
    return new org.apache.avro.Schema.Parser().parse(cdapSchema.toString());
  }

  // needed since avro uses their own utf8 class for strings
  private void assertMapEquals(Map<String, Object> expected, Map<Object, Object> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (Map.Entry<Object, Object> entry : actual.entrySet()) {
      Assert.assertEquals(expected.get(entry.getKey().toString()), entry.getValue());
    }
  }

  private ByteBuffer toByteBuffer(GenericRecord record) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    writer.write(record, encoder);
    encoder.flush();
    out.close();
    return ByteBuffer.wrap(out.toByteArray());
  }
}
