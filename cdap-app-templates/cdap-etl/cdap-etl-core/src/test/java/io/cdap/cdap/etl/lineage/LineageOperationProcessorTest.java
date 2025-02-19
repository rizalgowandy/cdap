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

package io.cdap.cdap.etl.lineage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.InputField;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.TransformOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldReadOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldWriteOperation;
import io.cdap.cdap.etl.proto.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LineageOperationProcessorTest {

  @Test
  public void testSimpleSourceToSinkPipeline() {
    // n1-->n2
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                "offset"));
    stageOperations.put("n2", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();
    Set<Operation> expected = new HashSet<>();
    expected.add(new ReadOperation("n1.read", "reading data",
                                   EndPoint.of("default", "file", ImmutableMap.of("stageName", "n1")),
                                   "offset"));
    expected.add(new WriteOperation("n2.write", "writing data",
                                    EndPoint.of("default", "file2", ImmutableMap.of("stageName", "n2")),
                                    InputField.of("n1.read", "offset")));

    Assert.assertEquals(new FieldLineageInfo(expected), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testSimpleSourceToSinkMultiPipelineWithUniqueFields() {
    // n1-->n2 where n1 is reading from multiple assets and n2 is writing to multiple assets
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read_a", "reading data", EndPoint.of("default", "file",
                                                                                     ImmutableMap.of("marker", "a")),
                                               "id1", "name"));
    fieldOperations.add(new FieldReadOperation("read_b", "reading data", EndPoint.of("default", "file",
                                                                                     ImmutableMap.of("marker", "b")),
                                               "id2", "description"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write_a", "writing data", EndPoint.of("default", "file2",
                                                                                       ImmutableMap.of("marker", "a")),
                                                "id1", "name"));
    fieldOperations.add(new FieldWriteOperation("write_b", "writing data", EndPoint.of("default", "file2",
                                                                                       ImmutableMap.of("marker", "b")),
                                                "id2", "description"));
    stageOperations.put("n2", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();
    Set<Operation> expected = new HashSet<>();
    expected.add(new ReadOperation("n1.read_a", "reading data",
                                   EndPoint.of("default", "file", ImmutableMap.of("marker", "a", "stageName", "n1")),
                                   "id1", "name"));
    expected.add(new ReadOperation("n1.read_b", "reading data",
                                   EndPoint.of("default", "file", ImmutableMap.of("marker", "b", "stageName", "n1")),
                                   "id2", "description"));
    expected.add(new WriteOperation("n2.write_a", "writing data",
                                    EndPoint.of("default", "file2", ImmutableMap.of("marker", "a", "stageName", "n2")),
                                    InputField.of("n1.read_a", "id1"), InputField.of("n1.read_a", "name")));
    expected.add(new WriteOperation("n2.write_b", "writing data",
                                    EndPoint.of("default", "file2", ImmutableMap.of("marker", "b", "stageName", "n2")),
                                    InputField.of("n1.read_b", "id2"), InputField.of("n1.read_b", "description")));

    Assert.assertEquals(new FieldLineageInfo(expected), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testSimpleSourceToSinkMultiPipelineWithDuplicateFields() {
    // n1-->n2 where n1 is reading from multiple assets and n2 is writing to multiple assets
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read_a", "reading data", EndPoint.of("default", "file",
                                                                                     ImmutableMap.of("marker", "a")),
                                               "id", "name"));
    fieldOperations.add(new FieldReadOperation("read_b", "reading data", EndPoint.of("default", "file",
                                                                                     ImmutableMap.of("marker", "b")),
                                               "id", "description"));
    fieldOperations.add(new FieldReadOperation("read_c", "reading data", EndPoint.of("default", "file",
                                                                                     ImmutableMap.of("marker", "c")),
                                               "id", "comment"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write_a", "writing data", EndPoint.of("default", "file2",
                                                                                       ImmutableMap.of("marker", "a")),
                                                "id", "name"));
    fieldOperations.add(new FieldWriteOperation("write_b", "writing data", EndPoint.of("default", "file2",
                                                                                       ImmutableMap.of("marker", "b")),
                                                "id", "description"));
    fieldOperations.add(new FieldWriteOperation("write_c", "writing data", EndPoint.of("default", "file2",
                                                                                       ImmutableMap.of("marker", "c")),
                                                "id", "comment"));
    stageOperations.put("n2", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();
      Set<Operation> expected = new HashSet<>();
    expected.add(new ReadOperation("n1.read_a", "reading data",
                                   EndPoint.of("default", "file", ImmutableMap.of("marker", "a", "stageName", "n1")),
                                   "id", "name"));
    expected.add(new ReadOperation("n1.read_b", "reading data",
                                   EndPoint.of("default", "file", ImmutableMap.of("marker", "b", "stageName", "n1")),
                                   "id", "description"));
    expected.add(new ReadOperation("n1.read_c", "reading data",
                                   EndPoint.of("default", "file", ImmutableMap.of("marker", "c", "stageName", "n1")),
                                   "id", "comment"));
    expected.add(new WriteOperation("n2.write_a", "writing data",
                                    EndPoint.of("default", "file2", ImmutableMap.of("marker", "a", "stageName", "n2")),
                                    InputField.of("n1.read_a", "id"), InputField.of("n1.read_a", "name")));
    expected.add(new WriteOperation("n2.write_b", "writing data",
                                    EndPoint.of("default", "file2", ImmutableMap.of("marker", "b", "stageName", "n2")),
                                    InputField.of("n1.read_b", "id"), InputField.of("n1.read_b", "description")));
    expected.add(new WriteOperation("n2.write_c", "writing data",
                                    EndPoint.of("default", "file2", ImmutableMap.of("marker", "c", "stageName", "n2")),
                                    InputField.of("n1.read_c", "id"), InputField.of("n1.read_c", "comment")));

    Assert.assertEquals(new FieldLineageInfo(expected), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testMergeOperationsNonRepeat() {
    // n1 -> n3 ----
    //           |---- n5
    // n2 -> n4 ----

    // operations (n1) -> (id, name)
    //            (n3) -> (body, offset)
    //            (n2.id) -> id
    //            (n2.name) -> name
    //            (n4.body) -> (id, name)
    //            (n5) -> (id, name)
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n3", "n5"));
    connections.add(new Connection("n2", "n4"));
    connections.add(new Connection("n4", "n5"));

    EndPoint src1 = EndPoint.of("default", "n1");
    EndPoint src2 = EndPoint.of("default", "n2");
    EndPoint dest = EndPoint.of("default", "n5");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("read1", "read description",
                                                                               src1, "id", "name")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("read2", "read description",
                                                                               src2, "body", "offset")));
    List<FieldOperation> n3Operations = stageOperations.computeIfAbsent("n3", k -> new ArrayList<>());
    n3Operations.add(new FieldTransformOperation("identity1", "identity", Collections.singletonList("id"), "id"));
    n3Operations.add(new FieldTransformOperation("identity2", "identity", Collections.singletonList("name"), "name"));

    stageOperations.put("n4", Collections.singletonList(new FieldTransformOperation("generate", "generate",
                                                                                    Collections.singletonList("body"),
                                                                                    "id", "name")));
    stageOperations.put("n5", Collections.singletonList(new FieldWriteOperation("write", "write", dest,
                                                                                "id", "name")));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());

    Set<Operation> expectedOperations = new HashSet<>();
    EndPoint expectedSrc1 = EndPoint.of("default", "n1", ImmutableMap.of("stageName", "n1"));
    EndPoint expectedSrc2 = EndPoint.of("default", "n2", ImmutableMap.of("stageName", "n2"));
    EndPoint expectedDest = EndPoint.of("default", "n5", ImmutableMap.of("stageName", "n5"));
    expectedOperations.add(new ReadOperation("n1.read1", "read description", expectedSrc1, "id", "name"));
    expectedOperations.add(new ReadOperation("n2.read2", "read description", expectedSrc2, "body", "offset"));
    expectedOperations.add(new TransformOperation("n3.identity1", "identity",
                                                  Collections.singletonList(InputField.of("n1.read1", "id")),
                                                  "id"));
    expectedOperations.add(new TransformOperation("n3.identity2", "identity",
                                                  Collections.singletonList(InputField.of("n1.read1", "name")),
                                                  "name"));
    expectedOperations.add(new TransformOperation("n4.generate", "generate",
                                                  Collections.singletonList(InputField.of("n2.read2", "body")),
                                                  "id", "name"));
    expectedOperations.add(new TransformOperation("n3,n4.merge.id", "Merged stages: n3,n4",
                                                  Arrays.asList(InputField.of("n3.identity1", "id"),
                                                                InputField.of("n4.generate", "id")), "id"));
    expectedOperations.add(new TransformOperation("n3,n4.merge.name", "Merged stages: n3,n4",
                                                  Arrays.asList(InputField.of("n3.identity2", "name"),
                                                                InputField.of("n4.generate", "name")), "name"));
    expectedOperations.add(new TransformOperation("n3,n4.merge.body", "Merged stages: n3,n4",
                                                  Collections.singletonList(InputField.of("n2.read2", "body")),
                                                  "body"));
    expectedOperations.add(new TransformOperation("n3,n4.merge.offset", "Merged stages: n3,n4",
                                                  Collections.singletonList(InputField.of("n2.read2", "offset")),
                                                  "offset"));
    expectedOperations.add(new WriteOperation("n5.write", "write", expectedDest,
                                              Arrays.asList(InputField.of("n3,n4.merge.id", "id"),
                                                            InputField.of("n3,n4.merge.name", "name"))));
    Set<Operation> process = processor.process();
    Assert.assertEquals(expectedOperations, process);
  }

  @Test
  public void testSameKeyAndRenameJoin() {
    //  n1(id(key), swap1, n1same) ---------
    //                              |
    //                            JOIN  ------->(id, new_id, swap1, swap2, n1same, n2same)
    //                              |
    //  n2(id(key), swap2, n2same)----------

    // operations (n1.id, n2.id) -> id
    //            (n2.id) -> new_id
    //            (n1.swap1) -> swap2
    //            (n2.swap2) -> swap1
    //            (n1.n1same) -> n1same
    //            (n2.n2same) -> n2same
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint src1 = EndPoint.of("default", "n1");
    EndPoint src2 = EndPoint.of("default", "n2");
    EndPoint dest = EndPoint.of("default", "n4");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("readSrc1", "read description",
                                                                               src1, "id", "swap1", "n1same")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("readSrc2", "read description",
                                                                               src2, "id", "swap2", "n2same")));
    List<FieldOperation> joinOperations = stageOperations.computeIfAbsent("n3", k -> new ArrayList<>());
    joinOperations.add(new FieldTransformOperation("JoinKey", "Join Key", Arrays.asList("n1.id", "n2.id"), "id"));
    joinOperations.add(new FieldTransformOperation("RenameN2", "rename", Collections.singletonList("n2.id"),
                                                   "new_id"));
    joinOperations.add(new FieldTransformOperation("swap1", "swap", Collections.singletonList("n1.swap1"), "swap2"));
    joinOperations.add(new FieldTransformOperation("swap2", "swap", Collections.singletonList("n2.swap2"), "swap1"));
    joinOperations.add(new FieldTransformOperation("unchange1", "unchange", Collections.singletonList("n1.n1same"),
                                                   "n1same"));
    joinOperations.add(new FieldTransformOperation("unchange2", "unchange", Collections.singletonList("n2.n2same"),
                                                   "n2same"));

    stageOperations.put("n4", Collections.singletonList(
      new FieldWriteOperation("Write", "write description",
                              dest, "id", "new_id", "swap1", "swap2", "n1same", "n2same")));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n3"));

    Set<Operation> expectedOperations = new HashSet<>();
    EndPoint expectedSrc1 = EndPoint.of("default", "n1", ImmutableMap.of("stageName", "n1"));
    EndPoint expectedSrc2 = EndPoint.of("default", "n2", ImmutableMap.of("stageName", "n2"));
    EndPoint expectedDest = EndPoint.of("default", "n4", ImmutableMap.of("stageName", "n4"));
    expectedOperations.add(new ReadOperation("n1.readSrc1", "read description", expectedSrc1, "id", "swap1", "n1same"));
    expectedOperations.add(new ReadOperation("n2.readSrc2", "read description", expectedSrc2, "id", "swap2", "n2same"));
    expectedOperations.add(new TransformOperation("n3.JoinKey", "Join Key",
                                                  Arrays.asList(InputField.of("n1.readSrc1", "id"),
                                                                InputField.of("n2.readSrc2", "id")), "id"));
    expectedOperations.add(new TransformOperation("n3.RenameN2", "rename",
                                                  Collections.singletonList(InputField.of("n2.readSrc2", "id")),
                                                  "new_id"));
    expectedOperations.add(new TransformOperation("n3.swap1", "swap",
                                                  Collections.singletonList(InputField.of("n1.readSrc1", "swap1")),
                                                  "swap2"));
    expectedOperations.add(new TransformOperation("n3.swap2", "swap",
                                                  Collections.singletonList(InputField.of("n2.readSrc2", "swap2")),
                                                  "swap1"));
    expectedOperations.add(new TransformOperation("n3.unchange1", "unchange",
                                                  Collections.singletonList(InputField.of("n1.readSrc1", "n1same")),
                                                  "n1same"));
    expectedOperations.add(new TransformOperation("n3.unchange2", "unchange",
                                                  Collections.singletonList(InputField.of("n2.readSrc2", "n2same")),
                                                  "n2same"));
    expectedOperations.add(new WriteOperation("n4.Write", "write description", expectedDest,
                                              Arrays.asList(InputField.of("n3.JoinKey", "id"),
                                                            InputField.of("n3.RenameN2", "new_id"),
                                                            InputField.of("n3.swap2", "swap1"),
                                                            InputField.of("n3.swap1", "swap2"),
                                                            InputField.of("n3.unchange1", "n1same"),
                                                            InputField.of("n3.unchange2", "n2same"))));
    Assert.assertEquals(expectedOperations, processor.process());
  }

  @Test
  public void testSimplePipelineWithTransform() {
    // n1-->n2-->n3
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset",
                                               "body"));
    stageOperations.put("n1", fieldOperations);
    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("parse", "parsing data", Collections.singletonList("body"),
                                                    Arrays.asList("name", "address", "zip")));
    stageOperations.put("n2", fieldOperations);
    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                "name", "address", "zip"));
    stageOperations.put("n3", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();
    Set<Operation> expected = new HashSet<>();
    expected.add(new ReadOperation("n1.read", "reading data",
                                   EndPoint.of("default", "file", ImmutableMap.of("stageName", "n1")),
                                   "offset", "body"));
    expected.add(new TransformOperation("n2.parse", "parsing data",
                                        Collections.singletonList(InputField.of("n1.read", "body")), "name", "address",
                                        "zip"));
    expected.add(new WriteOperation("n3.write", "writing data",
                                    EndPoint.of("default", "file2", ImmutableMap.of("stageName", "n3")),
                                    InputField.of("n2.parse", "name"), InputField.of("n2.parse", "address"),
                                    InputField.of("n2.parse", "zip")));

    Assert.assertEquals(new FieldLineageInfo(expected), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testAnotherSimplePipeline() {

    // n1-->n2-->n3-->n4
    // n1 => read: file -> (offset, body)
    // n2 => parse: (body) -> (first_name, last_name) | n2
    // n3 => concat: (first_name, last_name) -> (name) | n
    // n4 => write: (offset, name) -> another_file

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "some read", EndPoint.of("ns", "file1"), "offset",
                                               "body"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("parse", "parsing body", Collections.singletonList("body"),
                                                    "first_name", "last_name"));
    stageOperations.put("n2", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("concat", "concatinating the fields",
                                                    Arrays.asList("first_name", "last_name"), "name"));
    stageOperations.put("n3", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write_op", "writing data to file",
                                                EndPoint.of("myns", "another_file"),
                                                Arrays.asList("offset", "name")));
    stageOperations.put("n4", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();

    ReadOperation read = new ReadOperation("n1.read", "some read",
                                           EndPoint.of("ns", "file1", ImmutableMap.of("stageName", "n1")),
                                           "offset", "body");

    TransformOperation parse = new TransformOperation("n2.parse", "parsing body",
                                                      Collections.singletonList(InputField.of("n1.read", "body")),
                                                      "first_name", "last_name");

    TransformOperation concat = new TransformOperation("n3.concat", "concatinating the fields",
                                                       Arrays.asList(InputField.of("n2.parse", "first_name"),
                                                                     InputField.of("n2.parse", "last_name")),
                                                       "name");

    WriteOperation write = new WriteOperation("n4.write_op", "writing data to file",
                                              EndPoint.of("myns", "another_file", ImmutableMap.of("stageName", "n4")),
                                              Arrays.asList(InputField.of("n1.read", "offset"),
                                                            InputField.of("n3.concat", "name")));

    List<Operation> expectedOperations = new ArrayList<>();
    expectedOperations.add(parse);
    expectedOperations.add(concat);
    expectedOperations.add(read);
    expectedOperations.add(write);

    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testSourceWithMultipleDestinations() {
    //              |----->n3
    // n1--->n2-----|
    //              |----->n4

    // n1 => read: file -> (offset, body)
    // n2 => parse: body -> (id, name, address, zip)
    // n3 => write1: (parse.id, parse.name) -> info
    // n4 => write2: (parse.address, parse.zip) -> location

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint source = EndPoint.of("ns", "file");
    EndPoint info = EndPoint.of("ns", "info");
    EndPoint location = EndPoint.of("ns", "location");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "reading from file", source, "offset", "body"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("parse", "parsing body", Collections.singletonList("body"),
                                                    "id", "name", "address", "zip"));
    stageOperations.put("n2", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("infoWrite", "writing info", info, "id", "name"));
    stageOperations.put("n3", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("locationWrite", "writing location", location, "address", "zip"));
    stageOperations.put("n4", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();
    EndPoint expectedSource = EndPoint.of("ns", "file", ImmutableMap.of("stageName", "n1"));
    ReadOperation read = new ReadOperation("n1.read", "reading from file", expectedSource, "offset", "body");

    expectedOperations.add(read);

    TransformOperation parse = new TransformOperation("n2.parse", "parsing body",
                                                      Collections.singletonList(InputField.of("n1.read", "body")),
                                                      "id", "name", "address", "zip");

    expectedOperations.add(parse);
    EndPoint expectedInfo = EndPoint.of("ns", "info", ImmutableMap.of("stageName", "n3"));
    WriteOperation infoWrite = new WriteOperation("n3.infoWrite", "writing info", expectedInfo,
                                                  InputField.of("n2.parse", "id"),
                                                  InputField.of("n2.parse", "name"));

    expectedOperations.add(infoWrite);
    EndPoint expectedLocation = EndPoint.of("ns", "location", ImmutableMap.of("stageName", "n4"));
    WriteOperation locationWrite = new WriteOperation("n4.locationWrite", "writing location", expectedLocation,
                                                      InputField.of("n2.parse", "address"),
                                                      InputField.of("n2.parse", "zip"));

    expectedOperations.add(locationWrite);
    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testDirectMerge() {

    // n1--------->n3
    //       |
    // n2--------->n4

    // n1 => pRead: personFile -> (offset, body)
    // n2 => hRead: hrFile -> (offset, body)
    // n1.n2.merge => n1.n2.merge: (pRead.offset, pRead.body, hRead.offset, hRead.body) -> (offset, body)
    // n3 => write1: (n1.n2.merge.offset, n1.n2.merge.body) -> testStore
    // n4 => write1: (n1.n2.merge.offset, n1.n2.merge.body) -> prodStore

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n1", "n4"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n2", "n4"));

    EndPoint pEndPoint = EndPoint.of("ns", "personFile");
    EndPoint hEndPoint = EndPoint.of("ns", "hrFile");
    EndPoint testEndPoint = EndPoint.of("ns", "testStore");
    EndPoint prodEndPoint = EndPoint.of("ns", "prodStore");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("pRead", "Reading from person file", pEndPoint, "offset", "body"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("hRead", "Reading from hr file", hEndPoint, "offset", "body"));
    stageOperations.put("n2", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write1", "Writing to test store", testEndPoint, "offset",
                                                "body"));
    stageOperations.put("n3", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write2", "Writing to prod store", prodEndPoint, "offset",
                                                "body"));
    stageOperations.put("n4", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();
    EndPoint expectedPEndPoint = EndPoint.of("ns", "personFile", ImmutableMap.of("stageName", "n1"));
    EndPoint expectedHEndPoint = EndPoint.of("ns", "hrFile", ImmutableMap.of("stageName", "n2"));
    EndPoint expectedTestEndPoint = EndPoint.of("ns", "testStore", ImmutableMap.of("stageName", "n3"));
    EndPoint expectedProdEndPoint = EndPoint.of("ns", "prodStore", ImmutableMap.of("stageName", "n4"));
    ReadOperation pRead = new ReadOperation("n1.pRead", "Reading from person file", expectedPEndPoint, "offset",
                                            "body");
    expectedOperations.add(pRead);

    ReadOperation hRead = new ReadOperation("n2.hRead", "Reading from hr file", expectedHEndPoint, "offset", "body");
    expectedOperations.add(hRead);

    // implicit merge should be added by app
    TransformOperation merge1 = new TransformOperation("n1,n2.merge.offset", "Merged stages: n1,n2",
                                                      Arrays.asList(InputField.of("n1.pRead", "offset"),
                                                                    InputField.of("n2.hRead", "offset")),
                                                       "offset");
    TransformOperation merge2 = new TransformOperation("n1,n2.merge.body", "Merged stages: n1,n2",
                                                       Arrays.asList(InputField.of("n1.pRead", "body"),
                                                                     InputField.of("n2.hRead", "body")),
                                                       "body");
    expectedOperations.add(merge1);
    expectedOperations.add(merge2);

    WriteOperation write1 = new WriteOperation("n3.write1", "Writing to test store", expectedTestEndPoint,
                                               Arrays.asList(InputField.of("n1,n2.merge.offset", "offset"),
                                                             InputField.of("n1,n2.merge.body", "body")));
    expectedOperations.add(write1);

    WriteOperation write2 = new WriteOperation("n4.write2", "Writing to prod store", expectedProdEndPoint,
                                               Arrays.asList(InputField.of("n1,n2.merge.offset", "offset"),
                                                             InputField.of("n1,n2.merge.body", "body")));
    expectedOperations.add(write2);

    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testComplexMerge() {
    //
    //  n1----n2---
    //            |         |-------n6
    //            |----n5---|
    //  n3----n4---         |---n7----n8
    //
    //
    //  n1: read: file1 -> offset,body
    //  n2: parse: body -> name, address, zip
    //  n3: read: file2 -> offset,body
    //  n4: parse: body -> name, address, zip
    //  n5: normalize: address -> address
    //  n5: rename: address -> state_address
    //  n6: write: offset, name, address -> file3
    //  n7: rename: offset -> file_offset
    //  n8: write: file_offset, name, address, zip -> file4

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n5"));
    connections.add(new Connection("n3", "n4"));
    connections.add(new Connection("n4", "n5"));
    connections.add(new Connection("n5", "n6"));
    connections.add(new Connection("n5", "n7"));
    connections.add(new Connection("n7", "n8"));

    EndPoint n1EndPoint = EndPoint.of("ns", "file1");
    EndPoint n3EndPoint = EndPoint.of("ns", "file2");
    EndPoint n6EndPoint = EndPoint.of("ns", "file3");
    EndPoint n8EndPoint = EndPoint.of("ns", "file4");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "reading file 1", n1EndPoint, "offset", "body"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("parse", "parsing file 1", Collections.singletonList("body"),
                                                    "name", "address", "zip"));
    stageOperations.put("n2", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "reading file 2", n3EndPoint, "offset", "body"));
    stageOperations.put("n3", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("parse", "parsing file 2", Collections.singletonList("body"),
                                                    "name", "address", "zip"));
    stageOperations.put("n4", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("normalize", "normalizing address",
                                                    Collections.singletonList("address"), "address"));
    fieldOperations.add(new FieldTransformOperation("rename", "renaming address to state_address",
                                                    Collections.singletonList("address"), "state_address"));
    stageOperations.put("n5", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write", "writing file 3", n6EndPoint, "offset", "name",
                                                "address"));
    stageOperations.put("n6", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("rename", "renaming offset to file_offset",
                                                    Collections.singletonList("offset"), "file_offset"));
    stageOperations.put("n7", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write", "writing file 4", n8EndPoint, "file_offset", "name",
                                                "address", "zip"));
    stageOperations.put("n8", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();
    Set<Operation> expectedOperations = new HashSet<>();
    EndPoint expectedN1EndPoint = EndPoint.of("ns", "file1", ImmutableMap.of("stageName", "n1"));
    EndPoint expectedN3EndPoint = EndPoint.of("ns", "file2", ImmutableMap.of("stageName", "n3"));
    EndPoint expectedN6EndPoint = EndPoint.of("ns", "file3", ImmutableMap.of("stageName", "n6"));
    EndPoint expectedN8EndPoint = EndPoint.of("ns", "file4", ImmutableMap.of("stageName", "n8"));
    ReadOperation read = new ReadOperation("n1.read", "reading file 1", expectedN1EndPoint, "offset", "body");
    expectedOperations.add(read);

    TransformOperation parse = new TransformOperation("n2.parse", "parsing file 1",
                                                      Collections.singletonList(InputField.of("n1.read", "body")),
                                                      "name", "address", "zip");
    expectedOperations.add(parse);

    read = new ReadOperation("n3.read", "reading file 2", expectedN3EndPoint, "offset", "body");
    expectedOperations.add(read);

    parse = new TransformOperation("n4.parse", "parsing file 2",
                                   Collections.singletonList(InputField.of("n3.read", "body")), "name", "address",
                                   "zip");
    expectedOperations.add(parse);

    TransformOperation merge1 = new TransformOperation("n2,n4.merge.offset", "Merged stages: n2,n4",
                                                       ImmutableList.of(InputField.of("n1.read", "offset"),
                                                                        InputField.of("n3.read", "offset")), "offset");
    TransformOperation merge2 = new TransformOperation("n2,n4.merge.body", "Merged stages: n2,n4",
                                                       ImmutableList.of(InputField.of("n1.read", "body"),
                                                                        InputField.of("n3.read", "body")), "body");
    TransformOperation merge3 = new TransformOperation("n2,n4.merge.address", "Merged stages: n2,n4",
                                                       ImmutableList.of(InputField.of("n2.parse", "address"),
                                                                        InputField.of("n4.parse", "address")),
                                                       "address");
    TransformOperation merge4 = new TransformOperation("n2,n4.merge.name", "Merged stages: n2,n4",
                                                       ImmutableList.of(InputField.of("n2.parse", "name"),
                                                                        InputField.of("n4.parse", "name")), "name");
    TransformOperation merge5 = new TransformOperation("n2,n4.merge.zip", "Merged stages: n2,n4",
                                                       ImmutableList.of(InputField.of("n2.parse", "zip"),
                                                                        InputField.of("n4.parse", "zip")), "zip");

    expectedOperations.add(merge1);
    expectedOperations.add(merge2);
    expectedOperations.add(merge3);
    expectedOperations.add(merge4);
    expectedOperations.add(merge5);

    TransformOperation normalize = new TransformOperation("n5.normalize", "normalizing address",
                                                          Collections.singletonList(InputField.of("n2,n4.merge.address",
                                                                                                  "address")),
                                                          "address");
    expectedOperations.add(normalize);

    TransformOperation rename = new TransformOperation("n5.rename", "renaming address to state_address",
                                                       Collections.singletonList(InputField.of("n5.normalize",
                                                                                               "address")),
                                                       "state_address");
    expectedOperations.add(rename);

    WriteOperation write = new WriteOperation("n6.write", "writing file 3", expectedN6EndPoint,
                                              InputField.of("n2,n4.merge.offset", "offset"),
                                              InputField.of("n2,n4.merge.name", "name"),
                                              InputField.of("n5.normalize", "address"));
    expectedOperations.add(write);

    rename = new TransformOperation("n7.rename", "renaming offset to file_offset",
                                    Collections.singletonList(InputField.of("n2,n4.merge.offset", "offset")),
                                    "file_offset");
    expectedOperations.add(rename);

    write = new WriteOperation("n8.write", "writing file 4", expectedN8EndPoint,
                               InputField.of("n7.rename", "file_offset"), InputField.of("n2,n4.merge.name", "name"),
                               InputField.of("n5.normalize", "address"), InputField.of("n2,n4.merge.zip", "zip"));
    expectedOperations.add(write);

    Assert.assertEquals(expectedOperations, processedOperations);
  }

  @Test
  public void testSimpleJoinOperation() {
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint cEndPoint = EndPoint.of("default", "customer");
    EndPoint pEndPoint = EndPoint.of("default", "purchase");
    EndPoint cpEndPoint = EndPoint.of("default", "customer_purchase");


    //  customer -> (id)------------
    //                              |
    //                            JOIN  ------->(id, customer_id)
    //                              |
    //  purchase -> (customer_id)---

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("ReadCustomer", "read description",
                                                                               cEndPoint, "id")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("ReadPurchase", "read description",
                                                                               pEndPoint, "customer_id")));
    stageOperations.put("n3",
                        Collections.singletonList(new FieldTransformOperation("Join", "Join Operation",
                                                                              Arrays.asList("n1.id",
                                                                                            "n2.customer_id"),
                                                                              Arrays.asList("id", "customer_id"))));
    stageOperations.put("n4", Collections.singletonList(new FieldWriteOperation("Write", "write description",
                                                                                cpEndPoint, "id", "customer_id")));
    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n3"));
    Set<Operation> expectedOperations = new HashSet<>();
    EndPoint expectedCEndPoint = EndPoint.of("default", "customer", ImmutableMap.of("stageName", "n1"));
    EndPoint expectedPEndPoint = EndPoint.of("default", "purchase", ImmutableMap.of("stageName", "n2"));
    EndPoint expectedCPEndPoint = EndPoint.of("default", "customer_purchase", ImmutableMap.of("stageName", "n4"));
    expectedOperations.add(new ReadOperation("n1.ReadCustomer", "read description", expectedCEndPoint, "id"));
    expectedOperations.add(new ReadOperation("n2.ReadPurchase", "read description", expectedPEndPoint, "customer_id"));
    expectedOperations.add(new TransformOperation("n3.Join", "Join Operation",
                                                  Arrays.asList(InputField.of("n1.ReadCustomer", "id"),
                                                                InputField.of("n2.ReadPurchase", "customer_id")),
                                                  "id", "customer_id"));
    expectedOperations.add(new WriteOperation("n4.Write", "write description", expectedCPEndPoint,
                                              Arrays.asList(InputField.of("n3.Join", "id"),
                                                            InputField.of("n3.Join", "customer_id"))));
    Assert.assertEquals(expectedOperations, processor.process());
  }

  @Test
  public void testSimpleJoinWithAdditionalFields() {
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint cEndPoint = EndPoint.of("default", "customer");
    EndPoint pEndPoint = EndPoint.of("default", "purchase");
    EndPoint cpEndPoint = EndPoint.of("default", "customer_purchase");


    //  customer -> (id)------------
    //                              |
    //                            JOIN  ------->(id, customer_id)
    //                              |
    //  purchase -> (customer_id)---

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("ReadCustomer", "read description",
                                                                               cEndPoint, "id", "name")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("ReadPurchase", "read description",
                                                                               pEndPoint, "customer_id", "item")));

    List<FieldOperation> operationsFromJoin = new ArrayList<>();
    operationsFromJoin.add(new FieldTransformOperation("Join", "Join Operation",
                                                       Arrays.asList("n1.id", "n2.customer_id"),
                                                       Arrays.asList("id", "customer_id")));
    operationsFromJoin.add(new FieldTransformOperation("Identity name", "Identity Operation",
                                                       Collections.singletonList("n1.name"),
                                                       Collections.singletonList("name")));
    operationsFromJoin.add(new FieldTransformOperation("Identity item", "Identity Operation",
                                                       Collections.singletonList("n2.item"),
                                                       Collections.singletonList("item")));
    stageOperations.put("n3", operationsFromJoin);

    stageOperations.put("n4", Collections.singletonList(new FieldWriteOperation("Write", "write description",
                                                                                cpEndPoint, "id", "name",
                                                                                "customer_id", "item")));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n3"));

    Set<Operation> expectedOperations = new HashSet<>();
    EndPoint expectedCEndPoint = EndPoint.of("default", "customer", ImmutableMap.of("stageName", "n1"));
    EndPoint expectedPEndPoint = EndPoint.of("default", "purchase", ImmutableMap.of("stageName", "n2"));
    EndPoint expectedCPEndPoint = EndPoint.of("default", "customer_purchase", ImmutableMap.of("stageName", "n4"));
    expectedOperations.add(new ReadOperation("n1.ReadCustomer", "read description", expectedCEndPoint, "id", "name"));
    expectedOperations.add(new ReadOperation("n2.ReadPurchase", "read description", expectedPEndPoint, "customer_id",
                                             "item"));
    expectedOperations.add(new TransformOperation("n3.Join", "Join Operation",
                                                  Arrays.asList(InputField.of("n1.ReadCustomer", "id"),
                                                                InputField.of("n2.ReadPurchase", "customer_id")),
                                                  "id", "customer_id"));
    expectedOperations.add(new TransformOperation("n3.Identity name", "Identity Operation",
                                                  Collections.singletonList(InputField.of("n1.ReadCustomer", "name")),
                                                  "name"));
    expectedOperations.add(new TransformOperation("n3.Identity item", "Identity Operation",
                                                  Collections.singletonList(InputField.of("n2.ReadPurchase", "item")),
                                                  "item"));

    expectedOperations.add(new WriteOperation("n4.Write", "write description", expectedCPEndPoint,
                                              Arrays.asList(InputField.of("n3.Join", "id"),
                                                            InputField.of("n3.Identity name", "name"),
                                                            InputField.of("n3.Join", "customer_id"),
                                                            InputField.of("n3.Identity item", "item"))));
    Set<Operation> processedOperations = processor.process();
    Assert.assertEquals(expectedOperations, processedOperations);
  }

  @Test
  public void testSimpleJoinWithRenameJoinKeys() {
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint cEndPoint = EndPoint.of("default", "customer");
    EndPoint pEndPoint = EndPoint.of("default", "purchase");
    EndPoint cpEndPoint = EndPoint.of("default", "customer_purchase");


    //  customer -> (id, name)------------
    //                                    |
    //                                  JOIN  ------->(id_from_customer, id_from_purchase, name, item)
    //                                    |
    //  purchase -> (customer_id, item)---

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("ReadCustomer", "read description",
                                                                               cEndPoint, "id", "name")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("ReadPurchase", "read description",
                                                                               pEndPoint, "customer_id", "item")));

    List<FieldOperation> operationsFromJoin = new ArrayList<>();
    operationsFromJoin.add(new FieldTransformOperation("Join", "Join Operation",
                                                       Arrays.asList("n1.id", "n2.customer_id"),
                                                       Arrays.asList("id", "customer_id")));
    operationsFromJoin.add(new FieldTransformOperation("Rename id", "Rename id", Collections.singletonList("id"),
                                                       "id_from_customer"));
    operationsFromJoin.add(new FieldTransformOperation("Rename customer_id", "Rename customer_id",
                                                       Collections.singletonList("customer_id"), "id_from_purchase"));
    operationsFromJoin.add(new FieldTransformOperation("Identity name", "Identity Operation",
                                                       Collections.singletonList("n1.name"),
                                                       Collections.singletonList("name")));
    operationsFromJoin.add(new FieldTransformOperation("Identity item", "Identity Operation",
                                                       Collections.singletonList("n2.item"),
                                                       Collections.singletonList("item")));
    stageOperations.put("n3", operationsFromJoin);

    stageOperations.put("n4", Collections.singletonList(new FieldWriteOperation("Write", "write description",
                                                                                cpEndPoint, "id_from_customer",
                                                                                "id_from_purchase",
                                                                                "name", "item")));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n3"));
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();
    EndPoint expectedCEndPoint = EndPoint.of("default", "customer", ImmutableMap.of("stageName", "n1"));
    EndPoint expectedPEndPoint = EndPoint.of("default", "purchase", ImmutableMap.of("stageName", "n2"));
    EndPoint expectedCPEndPoint = EndPoint.of("default", "customer_purchase", ImmutableMap.of("stageName", "n4"));
    expectedOperations.add(new ReadOperation("n1.ReadCustomer", "read description", expectedCEndPoint, "id", "name"));
    expectedOperations.add(new ReadOperation("n2.ReadPurchase", "read description", expectedPEndPoint, "customer_id",
                                             "item"));
    expectedOperations.add(new TransformOperation("n3.Join", "Join Operation",
                                                  Arrays.asList(InputField.of("n1.ReadCustomer", "id"),
                                                                InputField.of("n2.ReadPurchase", "customer_id")),
                                                  "id", "customer_id"));
    expectedOperations.add(new TransformOperation("n3.Rename id", "Rename id",
                                                  Collections.singletonList(InputField.of("n3.Join", "id")),
                                                  "id_from_customer"));
    expectedOperations.add(new TransformOperation("n3.Rename customer_id", "Rename customer_id",
                                                  Collections.singletonList(InputField.of("n3.Join", "customer_id")),
                                                  "id_from_purchase"));
    expectedOperations.add(new TransformOperation("n3.Identity name", "Identity Operation",
                                                       Collections.singletonList(InputField.of("n1.ReadCustomer",
                                                                                               "name")), "name"));
    expectedOperations.add(new TransformOperation("n3.Identity item", "Identity Operation",
                                                  Collections.singletonList(InputField.of("n2.ReadPurchase", "item")),
                                                  "item"));

    expectedOperations.add(new WriteOperation("n4.Write", "write description", expectedCPEndPoint,
                                              Arrays.asList(InputField.of("n3.Rename id", "id_from_customer"),
                                                            InputField.of("n3.Rename customer_id", "id_from_purchase"),
                                                            InputField.of("n3.Identity name", "name"),
                                                            InputField.of("n3.Identity item", "item"))));

    Assert.assertEquals(expectedOperations, processedOperations);
  }

  @Test
  public void testSimpleJoinWithRenameOnAdditionalFields() {
    //  customer -> (id, name)----------
    //                                  |
    //                                JOIN  --->(id_from_customer, customer_id, name_from_customer, item_from_purchase)
    //                                  |
    //  purchase ->(customer_id, item)---

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint cEndPoint = EndPoint.of("default", "customer");
    EndPoint pEndPoint = EndPoint.of("default", "purchase");
    EndPoint cpEndPoint = EndPoint.of("default", "customer_purchase");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("ReadCustomer", "read description",
                                                                               cEndPoint, "id", "name")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("ReadPurchase", "read description",
                                                                               pEndPoint, "customer_id", "item")));

    List<FieldOperation> operationsFromJoin = new ArrayList<>();
    operationsFromJoin.add(new FieldTransformOperation("Join", "Join Operation",
                                                       Arrays.asList("n1.id", "n2.customer_id"),
                                                       Arrays.asList("id", "customer_id")));
    operationsFromJoin.add(new FieldTransformOperation("Rename id", "Rename id", Collections.singletonList("id"),
                                                       "id_from_customer"));
    operationsFromJoin.add(new FieldTransformOperation("Rename name", "Rename name",
                                                       Collections.singletonList("n1.name"), "name_from_customer"));
    operationsFromJoin.add(new FieldTransformOperation("Rename item", "Rename item",
                                                       Collections.singletonList("n2.item"), "item_from_purchase"));
    stageOperations.put("n3", operationsFromJoin);

    stageOperations.put("n4", Collections.singletonList(new FieldWriteOperation("Write", "write description",
                                                                                cpEndPoint, "id_from_customer",
                                                                                "customer_id", "name_from_customer",
                                                                                "item_from_purchase")));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n3"));
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();
    EndPoint expectedCEndPoint = EndPoint.of("default", "customer", ImmutableMap.of("stageName", "n1"));
    EndPoint expectedPEndPoint = EndPoint.of("default", "purchase", ImmutableMap.of("stageName", "n2"));
    EndPoint expectedCPEndPoint = EndPoint.of("default", "customer_purchase", ImmutableMap.of("stageName", "n4"));
    expectedOperations.add(new ReadOperation("n1.ReadCustomer", "read description", expectedCEndPoint, "id", "name"));
    expectedOperations.add(new ReadOperation("n2.ReadPurchase", "read description", expectedPEndPoint, "customer_id",
                                             "item"));
    expectedOperations.add(new TransformOperation("n3.Join", "Join Operation",
                                                  Arrays.asList(InputField.of("n1.ReadCustomer", "id"),
                                                                InputField.of("n2.ReadPurchase", "customer_id")),
                                                  "id", "customer_id"));
    expectedOperations.add(new TransformOperation("n3.Rename id", "Rename id",
                                                  Collections.singletonList(InputField.of("n3.Join", "id")),
                                                  "id_from_customer"));
    expectedOperations.add(new TransformOperation("n3.Rename name", "Rename name",
                                                  Collections.singletonList(InputField.of("n1.ReadCustomer",
                                                                                          "name")),
                                                  "name_from_customer"));
    expectedOperations.add(new TransformOperation("n3.Rename item", "Rename item",
                                                  Collections.singletonList(InputField.of("n2.ReadPurchase",
                                                                                          "item")),
                                                  "item_from_purchase"));

    expectedOperations.add(new WriteOperation("n4.Write", "write description", expectedCPEndPoint,
                                              Arrays.asList(InputField.of("n3.Rename id", "id_from_customer"),
                                                            InputField.of("n3.Join", "customer_id"),
                                                            InputField.of("n3.Rename name", "name_from_customer"),
                                                            InputField.of("n3.Rename item", "item_from_purchase"))));

    Assert.assertEquals(expectedOperations, processedOperations);
  }

  @Test
  public void testJoinWith3Inputs() {
    // customer -> (id, name)---------- |
    //                                  |
    // purchase ->(customer_id, item)------> JOIN --->(id_from_customer, customer_id, address_id,
    //                                  |                   name_from_customer, address)
    //                                  |
    // address ->(address_id, address)--|

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n4"));
    connections.add(new Connection("n2", "n4"));
    connections.add(new Connection("n3", "n4"));
    connections.add(new Connection("n4", "n5"));

    EndPoint cEndPoint = EndPoint.of("default", "customer");
    EndPoint pEndPoint = EndPoint.of("default", "purchase");
    EndPoint aEndPoint = EndPoint.of("default", "address");
    EndPoint acpEndPoint = EndPoint.of("default", "customer_purchase_address");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("ReadCustomer", "read description",
                                                                               cEndPoint, "id", "name")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("ReadPurchase", "read description",
                                                                               pEndPoint, "customer_id", "item")));
    stageOperations.put("n3", Collections.singletonList(new FieldReadOperation("ReadAddress", "read description",
                                                                               aEndPoint, "address_id", "address")));

    List<FieldOperation> operationsFromJoin = new ArrayList<>();

    operationsFromJoin.add(new FieldTransformOperation("Join", "Join Operation",
                                             Arrays.asList("n1.id", "n2.customer_id", "n3.address_id"),
                                             Arrays.asList("id", "customer_id", "address_id")));

    operationsFromJoin.add(new FieldTransformOperation("Rename id", "Rename Operation",
                                             Collections.singletonList("id"),
                                             Collections.singletonList("id_from_customer")));

    operationsFromJoin.add(new FieldTransformOperation("Rename customer.name", "Rename Operation",
                                             Collections.singletonList("n1.name"),
                                             Collections.singletonList("name_from_customer")));

    operationsFromJoin.add(new FieldTransformOperation("Identity address.address", "Identity Operation",
                                             Collections.singletonList("n3.address"),
                                             Collections.singletonList("address")));

    stageOperations.put("n4", operationsFromJoin);

    stageOperations.put("n5", Collections.singletonList(new FieldWriteOperation("Write", "Write Operation",
                                                                                acpEndPoint, "id_from_customer",
                                                                                "customer_id", "address_id",
                                                                                "name_from_customer",
                                                                                "address")));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n4"));
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();
    EndPoint expectedCEndPoint = EndPoint.of("default", "customer", ImmutableMap.of("stageName", "n1"));
    EndPoint expectedPEndPoint = EndPoint.of("default", "purchase", ImmutableMap.of("stageName", "n2"));
    EndPoint expectedAEndPoint = EndPoint.of("default", "address", ImmutableMap.of("stageName", "n3"));
    EndPoint expectedACPEndPoint = EndPoint.of("default", "customer_purchase_address",
                                               ImmutableMap.of("stageName", "n5"));
    expectedOperations.add(new ReadOperation("n1.ReadCustomer", "read description", expectedCEndPoint, "id", "name"));
    expectedOperations.add(new ReadOperation("n2.ReadPurchase", "read description", expectedPEndPoint, "customer_id",
                                             "item"));
    expectedOperations.add(new ReadOperation("n3.ReadAddress", "read description", expectedAEndPoint, "address_id",
                                             "address"));

    expectedOperations.add(new TransformOperation("n4.Join", "Join Operation",
                                                  Arrays.asList(InputField.of("n1.ReadCustomer", "id"),
                                                                InputField.of("n2.ReadPurchase", "customer_id"),
                                                                InputField.of("n3.ReadAddress", "address_id")),
                                                  "id", "customer_id", "address_id"));

    expectedOperations.add(new TransformOperation("n4.Rename id", "Rename Operation",
                                                  Collections.singletonList(InputField.of("n4.Join", "id")),
                                                  "id_from_customer"));

    expectedOperations.add(new TransformOperation("n4.Rename customer.name", "Rename Operation",
                                                  Collections.singletonList(InputField.of("n1.ReadCustomer",
                                                                                          "name")),
                                                  "name_from_customer"));

    expectedOperations.add(new TransformOperation("n4.Identity address.address", "Identity Operation",
                                                  Collections.singletonList(InputField.of("n3.ReadAddress",
                                                                                          "address")),
                                                  "address"));

    expectedOperations.add(new WriteOperation("n5.Write", "Write Operation", expectedACPEndPoint,
                                              Arrays.asList(InputField.of("n4.Rename id", "id_from_customer"),
                                                            InputField.of("n4.Join", "customer_id"),
                                                            InputField.of("n4.Join", "address_id"),
                                                            InputField.of("n4.Rename customer.name",
                                                                          "name_from_customer"),
                                                            InputField.of("n4.Identity address.address", "address"))));

    Assert.assertEquals(expectedOperations, processedOperations);

  }
}
