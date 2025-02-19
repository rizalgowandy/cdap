/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.common.namespace;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link DefaultNamespacePathLocator}.
 */
public class DefaultNamespacePathLocatorTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testGet() throws Exception {
    File locationFactoryPath = TEMP_FOLDER.newFolder();
    LocationFactory locationFactory = new LocalLocationFactory(locationFactoryPath);

    NamespaceAdmin nsAdmin = new InMemoryNamespaceAdmin();

    NamespaceId ns1 = new NamespaceId("ns1");
    NamespaceMeta defaultNSMeta = new NamespaceMeta.Builder().setName(NamespaceId.DEFAULT).build();
    NamespaceMeta ns1NSMeta = new NamespaceMeta.Builder().setName(ns1).setRootDirectory("/ns1").build();

    nsAdmin.create(defaultNSMeta);
    nsAdmin.create(ns1NSMeta);

    CConfiguration cConf = CConfiguration.create();
    NamespacePathLocator namespacePathLocator =
      new DefaultNamespacePathLocator(cConf, locationFactory, nsAdmin);

    Location defaultLoc = namespacePathLocator.get(NamespaceId.DEFAULT);
    Location ns1Loc = namespacePathLocator.get(ns1);

    // check if location was as expected
    Location expectedLocation = locationFactory.create(cConf.get(Constants.Namespace.NAMESPACES_DIR))
      .append(NamespaceId.DEFAULT.getNamespace());
    Assert.assertEquals(expectedLocation, defaultLoc);
    expectedLocation = Locations.getLocationFromAbsolutePath(locationFactory, "/ns1");
    Assert.assertEquals(expectedLocation, ns1Loc);

    // test these are not the same
    Assert.assertNotEquals(defaultLoc, ns1Loc);

    // test subdirectories in a namespace
    Location sub1 = namespacePathLocator.get(ns1).append("sub1");
    Location sub2 = namespacePathLocator.get(ns1).append("sub2");
    Assert.assertNotEquals(sub1, sub2);
  }
}
