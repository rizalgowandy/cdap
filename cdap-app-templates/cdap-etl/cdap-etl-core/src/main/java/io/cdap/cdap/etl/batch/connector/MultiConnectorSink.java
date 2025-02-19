/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.connector;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Connector sink that needs to preserve which stage each record came from and the record type. This
 * is used in the MapReduce engine, where connectors can store output from multiple stages.
 * Connectors store the stage name each record came from in case they are placed in front of a
 * joiner.
 */
public class MultiConnectorSink extends ConnectorSink<RecordInfo<StructuredRecord>> {

  public MultiConnectorSink(String datasetName, String phaseName) {
    super(datasetName, phaseName);
  }

  @Override
  public void transform(RecordInfo<StructuredRecord> input,
      Emitter<KeyValue<NullWritable, Text>> emitter)
      throws Exception {
    StructuredRecord modifiedRecord = modifyRecord(input);
    emitter.emit(new KeyValue<>(NullWritable.get(),
        new Text(StructuredRecordStringConverter.toJsonString(modifiedRecord))));
  }

  private StructuredRecord modifyRecord(RecordInfo<StructuredRecord> input) throws IOException {
    String stageName = input.getFromStage();
    Schema inputSchema = input.getValue().getSchema();
    return StructuredRecord.builder(MultiConnectorSource.RECORD_WITH_SCHEMA)
        .set("stageName", stageName)
        .set("type", input.getType().name())
        .set("schema", inputSchema.toString())
        .set("record", StructuredRecordStringConverter.toJsonString(input.getValue()))
        .build();
  }
}
