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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.DatasetMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatasetInstanceServiceTest extends DatasetServiceTestBase {

  @BeforeClass
  public static void setup() throws Exception {
    DatasetServiceTestBase.initialize();
  }

  @Test
  public void testInstanceMetaCache() throws Exception {

    // deploy a dataset
    instanceService.create(NamespaceId.DEFAULT.getEntityName(), "testds",
                           new DatasetInstanceConfiguration("table", new HashMap<>()));

    // get the dataset meta for two different owners, assert it is the same
    DatasetMeta meta = instanceService.get(NamespaceId.DEFAULT.dataset("testds"));
    DatasetMeta met2 = instanceService.get(NamespaceId.DEFAULT.dataset("testds"));
    Assert.assertSame(meta, met2);

    // update the dataset
    instanceService.update(NamespaceId.DEFAULT.dataset("testds"),
                           ImmutableMap.of("ttl", "12345678"));

    // get the dataset meta, validate it changed
    met2 = instanceService.get(NamespaceId.DEFAULT.dataset("testds"));
    Assert.assertNotSame(meta, met2);
    Assert.assertEquals("12345678", met2.getSpec().getProperty("ttl"));

    // delete the dataset
    instanceService.drop(NamespaceId.DEFAULT.dataset("testds"));

    // get the dataset meta, validate not found
    try {
      instanceService.get(NamespaceId.DEFAULT.dataset("testds"));
      Assert.fail("get() should have thrown NotFoundException");
    } catch (NotFoundException e) {
      // expected
    }

    // recreate the dataset
    instanceService.create(NamespaceId.DEFAULT.getNamespace(), "testds",
                           new DatasetInstanceConfiguration("table", new HashMap<>()));

    // get the dataset meta, validate it is up to date
    met2 = instanceService.get(NamespaceId.DEFAULT.dataset("testds"));
    Assert.assertEquals(meta.getSpec(), met2.getSpec());
  }

}
