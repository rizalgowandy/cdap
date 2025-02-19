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

package io.cdap.cdap.common.io;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link Locations}
 */
public class LocationsTest {
  private static final String TEST_BASE_PATH = "test_base";
  private static final String TEST_PATH = "some/test/path";

  @Test
  public void testParentHDFS() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://1.2.3.4:8020");
    LocationFactory locationFactory = new FileContextLocationFactory(conf, "abc");
    Location location = locationFactory.create("def");
    Assert.assertEquals("def", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertEquals("abc", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertTrue(Locations.isRoot(location));
    Assert.assertTrue(location.getName().isEmpty());
    Assert.assertNull(Locations.getParent(location));
  }

  @Test
  public void testRootParentFile() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    LocationFactory locationFactory = new FileContextLocationFactory(conf, "abc");
    Location location = locationFactory.create("def");
    Assert.assertEquals("def", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertEquals("abc", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertTrue(Locations.isRoot(location));
    Assert.assertTrue(location.getName().isEmpty());
    Assert.assertNull(Locations.getParent(location));
  }

  @Test
  public void testRootParentLocal() throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(new File(File.separator));
    Location location = locationFactory.create("abc");
    location = location.append("def");
    Assert.assertEquals("def", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertEquals("abc", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertTrue(Locations.isRoot(location));
    Assert.assertTrue(location.getName().isEmpty());
    Assert.assertNull(Locations.getParent(location));
  }

  @Test
  public void absolutePathTests() {
    // Test HDFS:
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://1.2.3.4:8020/");
    LocationFactory locationFactory = new FileContextLocationFactory(conf, TEST_BASE_PATH);
    Location location1 = locationFactory.create(TEST_PATH);
    Location location2 = Locations.getLocationFromAbsolutePath(locationFactory, location1.toURI().getPath());
    Assert.assertEquals(location1.toURI(), location2.toURI());

    // Test file:
    conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    locationFactory = new FileContextLocationFactory(conf, TEST_BASE_PATH);
    location1 = locationFactory.create(TEST_PATH);
    location2 = Locations.getLocationFromAbsolutePath(locationFactory, location1.toURI().getPath());
    Assert.assertEquals(location1.toURI(), location2.toURI());

    // Test LocalLocation
    locationFactory = new LocalLocationFactory(new File(TEST_BASE_PATH));
    location1 = locationFactory.create(TEST_PATH);
    location2 = Locations.getLocationFromAbsolutePath(locationFactory, location1.toURI().getPath());
    Assert.assertEquals(location1.toURI(), location2.toURI());
  }

  @Test
  public void testLocationComparator() {
    LocationFactory locationFactory = new LocalLocationFactory(new File(File.separator));
    Object[][] testCases = new Object[][] {
        {"/parent1/child1", "/parent1/child1", 0},
        {"/parent1/child1/", "/parent1/child1/", 0},
        {"/parent1/child1", "/parent1/child1/", 0},
        {"/parent1/child1/", "/parent1/child1", 0},
        {"/parent1/child", "/parent1/child1", -1},
        {"/parent1/child1", "/parent1/child", 1}};

    for (Object[] testCase : testCases) {
      Location location1 = locationFactory.create((String) testCase[0]);
      Location location2 = locationFactory.create((String) testCase[1]);
      int expected = (int) testCase[2];
      Assert.assertEquals(expected, Locations.LOCATION_COMPARATOR.compare(location1, location2));
    }
  }
}


