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

package io.cdap.cdap.etl.proto.v2;

import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for DataStreamsConfig
 */
public class DataStreamsConfigTest {

  @Test
  public void testEmptyCheckpointDir() {
    DataStreamsConfig config = DataStreamsConfig.builder()
      .setCheckpointDir("")
      .addStage(new ETLStage("source", new ETLPlugin("name", "type", Collections.emptyMap())))
      .addStage(new ETLStage("sink", new ETLPlugin("name", "type", Collections.emptyMap())))
      .addConnection("source", "sink")
      .build();
    Assert.assertNull(config.getCheckpointDir());
  }
}
