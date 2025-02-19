/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.messaging.store.leveldb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.MetadataTableTest;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link LevelDBMetadataTable}.
 */
public class LevelDBMetadataTableTest extends MetadataTableTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static TableFactory tableFactory;

  @BeforeClass
  public static void init() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    tableFactory = new LevelDBTableFactory(cConf);
  }

  @Override
  protected MetadataTable createMetadataTable() throws Exception {
    return tableFactory.createMetadataTable();
  }

  @Test
  public void testScanTopics() throws Exception {
    try (MetadataTable metadataTable = createMetadataTable()) {
      LevelDBMetadataTable table = (LevelDBMetadataTable) metadataTable;
      TopicMetadata t1 =
          new DefaultTopicMetadata(
              NamespaceId.CDAP.topic("t1"),
              ImmutableMap.of(
                  DefaultTopicMetadata.TTL_KEY, "10", DefaultTopicMetadata.GENERATION_KEY, "1"));
      TopicMetadata t2 =
          new DefaultTopicMetadata(
              NamespaceId.SYSTEM.topic("t2"),
              ImmutableMap.of(
                  DefaultTopicMetadata.TTL_KEY, "20", DefaultTopicMetadata.GENERATION_KEY, "1"));
      metadataTable.createTopic(t1);
      metadataTable.createTopic(t2);
      List<TopicId> allTopics = table.listTopics();
      Assert.assertEquals(2, allTopics.size());
      List<TopicMetadata> metadatas = new ArrayList<>();
      Iterators.addAll(metadatas, table.scanTopics());
      Assert.assertEquals(2, metadatas.size());

      allTopics = table.listTopics(NamespaceId.CDAP);
      Assert.assertEquals(1, allTopics.size());
      allTopics = table.listTopics(NamespaceId.SYSTEM);
      Assert.assertEquals(1, allTopics.size());

      metadataTable.deleteTopic(t1.getTopicId());

      metadatas.clear();
      Iterators.addAll(metadatas, table.scanTopics());
      Assert.assertEquals(2, metadatas.size());

      Assert.assertEquals(1, metadataTable.listTopics().size());
      Assert.assertEquals(1, metadataTable.listTopics(NamespaceId.SYSTEM).size());
      Assert.assertTrue(metadataTable.listTopics(NamespaceId.CDAP).isEmpty());

      metadataTable.deleteTopic(t2.getTopicId());
      metadatas.clear();
      Iterators.addAll(metadatas, table.scanTopics());

      for (TopicMetadata metadata : metadatas) {
        Assert.assertEquals(-1, metadata.getGeneration());
      }

      Assert.assertTrue(metadataTable.listTopics().isEmpty());
    }
  }
}
