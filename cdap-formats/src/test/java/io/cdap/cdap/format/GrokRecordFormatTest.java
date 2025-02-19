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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.format.Formats;
import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class GrokRecordFormatTest {

  @Test
  public void testSimple() throws Exception {
    Schema schema = Schema.recordOf(
      "streamEvent",
      Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    FormatSpecification spec = new FormatSpecification(Formats.GROK, schema,
                                                       GrokRecordFormat.settings("%{USER:user}:%{GREEDYDATA:body}"));
    RecordFormat<ByteBuffer, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);

    String message = "nitin:falkfjaksjf fkafjalkf fa fasfsalfsaf af afaslkfjasf asf af asf";
    StructuredRecord record = format.read(ByteBuffer.wrap(Bytes.toBytes(message)));
    Assert.assertEquals("nitin", record.get("user"));
    Assert.assertEquals("falkfjaksjf fkafjalkf fa fasfsalfsaf af afaslkfjasf asf af asf", record.get("body"));
  }

  @Test
  public void testDefault() throws Exception {
    FormatSpecification spec = new FormatSpecification(Formats.GROK, null,
                                                       GrokRecordFormat.settings("%{GREEDYDATA:body}"));
    RecordFormat<ByteBuffer, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);

    String message = "Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over";
    StructuredRecord record = format.read(ByteBuffer.wrap(Bytes.toBytes(message)));
    Assert.assertEquals("Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over", record.get("body"));
  }

  @Test
  public void testSyslog() throws Exception {
    FormatSpecification spec = new FormatSpecification(Formats.SYSLOG, null, Collections.<String, String>emptyMap());
    RecordFormat<ByteBuffer, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);

    String message = "Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over";
    StructuredRecord record = format.read(ByteBuffer.wrap(Bytes.toBytes(message)));
    Assert.assertEquals("Oct 17 08:59:00", record.get("timestamp"));
    Assert.assertEquals("suod", record.get("logsource"));
    Assert.assertEquals("newsyslog", record.get("program"));
    Assert.assertEquals("6215", record.get("pid"));
    Assert.assertEquals("logfile turned over", record.get("message"));

    message = "Oct 17 08:59:04 cdr.cs.colorado.edu amd[29648]: "
        + "noconn option exists, and was turned on! (May cause NFS hangs on some systems...)";
    record = format.read(ByteBuffer.wrap(Bytes.toBytes(message)));
    Assert.assertEquals("Oct 17 08:59:04", record.get("timestamp"));
    Assert.assertEquals("cdr.cs.colorado.edu", record.get("logsource"));
    Assert.assertEquals("amd", record.get("program"));
    Assert.assertEquals("29648", record.get("pid"));
    Assert.assertEquals(
        "noconn option exists, and was turned on! (May cause NFS hangs on some systems...)",
        record.get("message"));
  }

}
