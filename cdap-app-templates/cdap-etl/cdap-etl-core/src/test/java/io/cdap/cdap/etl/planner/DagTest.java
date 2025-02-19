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

package io.cdap.cdap.etl.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.etl.proto.Connection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class DagTest {

  @Test
  public void testTopologicalOrder() {
    // n1 -> n2 -> n3 -> n4
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"), new Connection("n2", "n3"), new Connection("n3", "n4")));
    Assert.assertEquals(ImmutableList.of("n1", "n2", "n3", "n4"), dag.getTopologicalOrder());

    /*
             |--- n2 ---|
        n1 --|          |-- n4
             |--- n3 ---|
     */
    dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3"),
      new Connection("n2", "n4"),
      new Connection("n3", "n4")));
    // could be n1 -> n2 -> n3 -> n4
    // or it could be n1 -> n3 -> n2 -> n4
    List<String> linearized = dag.getTopologicalOrder();
    Assert.assertEquals("n1", linearized.get(0));
    Assert.assertEquals("n4", linearized.get(3));
    assertBefore(linearized, "n1", "n2");
    assertBefore(linearized, "n1", "n3");

    /*
        n1 --|
             |--- n3
        n2 --|
     */
    dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n3"),
      new Connection("n2", "n3")));
    // could be n1 -> n2 -> n3
    // or it could be n2 -> n1 -> n3
    linearized = dag.getTopologicalOrder();
    Assert.assertEquals("n3", linearized.get(2));
    assertBefore(linearized, "n1", "n3");
    assertBefore(linearized, "n2", "n3");

    /*
                                     |--- n3
             |--- n2 ----------------|
        n1 --|       |               |--- n5
             |--------- n4 ----------|
             |              |        |
             |---------------- n6 ---|

        vertical arrows are pointing down
     */
    dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n4"),
      new Connection("n1", "n6"),
      new Connection("n2", "n3"),
      new Connection("n2", "n4"),
      new Connection("n2", "n5"),
      new Connection("n4", "n3"),
      new Connection("n4", "n5"),
      new Connection("n4", "n6"),
      new Connection("n6", "n3"),
      new Connection("n6", "n5")));
    linearized = dag.getTopologicalOrder();
    Assert.assertEquals("n1", linearized.get(0));
    Assert.assertEquals("n2", linearized.get(1));
    Assert.assertEquals("n4", linearized.get(2));
    Assert.assertEquals("n6", linearized.get(3));
    assertBefore(linearized, "n6", "n3");
    assertBefore(linearized, "n6", "n5");
  }

  private void assertBefore(List<String> list, String a, String b) {
    int aIndex = list.indexOf(a);
    int bIndex = list.indexOf(b);
    Assert.assertTrue(aIndex < bIndex);
  }

  @Test(expected = IllegalStateException.class)
  public void testCycle() {
    /*
             |---> n2 ----|
             |      ^     v
        n1 --|      |     n3 --> n5
             |---> n4 <---|
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n2"),
      new Connection("n3", "n5")));
    dag.getTopologicalOrder();
  }

  @Test
  public void testRemoveSource() {
    /*
             |--- n2 ---|
        n1 --|          |-- n4
             |--- n3 ---|
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3"),
      new Connection("n2", "n4"),
      new Connection("n3", "n4")));
    Assert.assertEquals("n1", dag.removeSource());
    Assert.assertEquals(ImmutableSet.of("n2", "n3"), dag.getSources());

    Set<String> removed = ImmutableSet.of(dag.removeSource(), dag.removeSource());
    Assert.assertEquals(ImmutableSet.of("n2", "n3"), removed);
    Assert.assertEquals(ImmutableSet.of("n4"), dag.getSources());
    Assert.assertEquals("n4", dag.removeSource());
    Assert.assertTrue(dag.getSources().isEmpty());
    Assert.assertTrue(dag.getSinks().isEmpty());
    Assert.assertNull(dag.removeSource());
  }

  @Test
  public void testIslands() {
    /*
        n1 -- n2

        n3 -- n4
     */
    try {
      new Dag(ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n3", "n4")));
      Assert.fail();
    } catch (DisjointConnectionsException e) {
      // expected
      Assert.assertTrue(e.getMessage().startsWith("Invalid DAG. There is an island"));
    }

    /*
        n1 -- n2
              |
              v
        n3 -- n4
              ^
        n5----|   n6 -- n7
     */
    try {
      new Dag(ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n2", "n4"),
        new Connection("n3", "n4"),
        new Connection("n5", "n4"),
        new Connection("n6", "n7")));
      Assert.fail();
    } catch (DisjointConnectionsException e) {
      // expected
      Assert.assertTrue(e.getMessage().startsWith("Invalid DAG. There is an island"));
    }
  }

  @Test
  public void testBranches() {
    /*
                              |--> n4
             |--> n2 --> n3 --|
        n1 --|                |--|
             |--|                |--> n5 --> n6 --> n9
                |--> n8 ---------|
        n7 -----|
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n8"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n3", "n5"),
      new Connection("n5", "n6"),
      new Connection("n7", "n8"),
      new Connection("n8", "n5"),
      new Connection("n6", "n9")));
    Set<String> stopNodes = new HashSet<>();
    Assert.assertEquals(ImmutableList.of("n1"), dag.getBranch("n1", stopNodes));
    Assert.assertEquals(ImmutableList.of("n2"), dag.getBranch("n2", stopNodes));
    Assert.assertEquals(ImmutableList.of("n2", "n3"), dag.getBranch("n3", stopNodes));
    Assert.assertEquals(ImmutableList.of("n4"), dag.getBranch("n4", stopNodes));
    Assert.assertEquals(ImmutableList.of("n5"), dag.getBranch("n5", stopNodes));
    Assert.assertEquals(ImmutableList.of("n5", "n6"), dag.getBranch("n6", stopNodes));
    Assert.assertEquals(ImmutableList.of("n7"), dag.getBranch("n7", stopNodes));
    Assert.assertEquals(ImmutableList.of("n8"), dag.getBranch("n8", stopNodes));
    Assert.assertEquals(ImmutableList.of("n5", "n6", "n9"), dag.getBranch("n9", stopNodes));
    stopNodes = ImmutableSet.of("n6", "n3", "n8");
    Assert.assertEquals(ImmutableList.of("n1"), dag.getBranch("n1", stopNodes));
    Assert.assertEquals(ImmutableList.of("n2"), dag.getBranch("n2", stopNodes));
    Assert.assertEquals(ImmutableList.of("n2", "n3"), dag.getBranch("n3", stopNodes));
    Assert.assertEquals(ImmutableList.of("n4"), dag.getBranch("n4", stopNodes));
    Assert.assertEquals(ImmutableList.of("n5"), dag.getBranch("n5", stopNodes));
    Assert.assertEquals(ImmutableList.of("n5", "n6"), dag.getBranch("n6", stopNodes));
    Assert.assertEquals(ImmutableList.of("n7"), dag.getBranch("n7", stopNodes));
    Assert.assertEquals(ImmutableList.of("n8"), dag.getBranch("n8", stopNodes));
    Assert.assertEquals(ImmutableList.of("n6", "n9"), dag.getBranch("n9", stopNodes));
  }

  @Test
  public void testAccessibleFrom() {
    /*
        n1 -- n2
              |
              v
        n3 -- n4 --- n8
              ^
              |
        n5-------- n6 -- n7
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n4"),
      new Connection("n3", "n4"),
      new Connection("n4", "n8"),
      new Connection("n5", "n4"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7")));
    Assert.assertEquals(ImmutableSet.of("n1", "n2", "n4", "n8"), dag.accessibleFrom("n1"));
    Assert.assertEquals(ImmutableSet.of("n2", "n4", "n8"), dag.accessibleFrom("n2"));
    Assert.assertEquals(ImmutableSet.of("n3", "n4", "n8"), dag.accessibleFrom("n3"));
    Assert.assertEquals(ImmutableSet.of("n4", "n8"), dag.accessibleFrom("n4"));
    Assert.assertEquals(ImmutableSet.of("n5", "n4", "n8", "n6", "n7"), dag.accessibleFrom("n5"));
    Assert.assertEquals(ImmutableSet.of("n6", "n7"), dag.accessibleFrom("n6"));
    Assert.assertEquals(ImmutableSet.of("n7"), dag.accessibleFrom("n7"));
    Assert.assertEquals(ImmutableSet.of("n8"), dag.accessibleFrom("n8"));

    // this stop node isn't accessible from n5 anyway, shouldn't have any effect
    Assert.assertEquals(ImmutableSet.of("n5", "n4", "n8", "n6", "n7"),
                        dag.accessibleFrom("n5", ImmutableSet.of("n1")));
    // these stop nodes cut off all paths, though the stop nodes themselves should show up in accessible set
    Assert.assertEquals(ImmutableSet.of("n5", "n4", "n6"), dag.accessibleFrom("n5", ImmutableSet.of("n4", "n6")));
    // these stop nodes cut off some paths
    Assert.assertEquals(ImmutableSet.of("n5", "n4", "n6", "n7"),
                        dag.accessibleFrom("n5", ImmutableSet.of("n4")));
    // these stop nodes cut off some paths
    Assert.assertEquals(ImmutableSet.of("n5", "n4", "n6", "n8"),
                        dag.accessibleFrom("n5", ImmutableSet.of("n6", "n1")));
  }

  @Test
  public void testParentsOf() {
    /*
        n1 -> n2
              |
              v
        n3 -> n4 --> n8
              ^
              |
        n5-------> n6 -> n7
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n4"),
      new Connection("n3", "n4"),
      new Connection("n4", "n8"),
      new Connection("n5", "n4"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7")));
    Assert.assertEquals(ImmutableSet.of("n1"), dag.parentsOf("n1"));
    Assert.assertEquals(ImmutableSet.of("n1", "n2"), dag.parentsOf("n2"));
    Assert.assertEquals(ImmutableSet.of("n3"), dag.parentsOf("n3"));
    Assert.assertEquals(ImmutableSet.of("n1", "n2", "n3", "n4", "n5"), dag.parentsOf("n4"));
    Assert.assertEquals(ImmutableSet.of("n5"), dag.parentsOf("n5"));
    Assert.assertEquals(ImmutableSet.of("n5", "n6"), dag.parentsOf("n6"));
    Assert.assertEquals(ImmutableSet.of("n5", "n6", "n7"), dag.parentsOf("n7"));
    Assert.assertEquals(ImmutableSet.of("n1", "n2", "n3", "n4", "n5", "n8"), dag.parentsOf("n8"));

    // these stop nodes are not parents, shouldn't have any effect
    Assert.assertEquals(ImmutableSet.of("n1", "n2", "n3", "n4", "n5", "n8"),
                        dag.parentsOf("n8", ImmutableSet.of("n6", "n7")));
    Assert.assertEquals(ImmutableSet.of("n5", "n6", "n7"),
                        dag.parentsOf("n7", ImmutableSet.of("n4")));
    // these stop nodes cut off all paths except itself
    Assert.assertEquals(ImmutableSet.of("n6", "n7"), dag.parentsOf("n7", ImmutableSet.of("n5", "n6")));
    // these stop nodes cut off some paths
    Assert.assertEquals(ImmutableSet.of("n2", "n3", "n5", "n4", "n8"),
                        dag.parentsOf("n8", ImmutableSet.of("n2", "n3")));
    Assert.assertEquals(ImmutableSet.of("n2", "n3", "n4", "n5"),
                        dag.parentsOf("n4", ImmutableSet.of("n2", "n3")));
  }

  @Test
  public void testSubsetAround() {
    /*
         n1 --> n2 --|
                     |--> n3 --> n4 --|
         n7 --> n8 --|                |--> n5 --> n6
                                      |
         n9 --------------------------|
     */
    Dag fullDag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6"),
      new Connection("n7", "n8"),
      new Connection("n8", "n3"),
      new Connection("n9", "n5")));

    // test without stop nodes

    /*
         n1 --> n2 --|
                     |--> n3 --> n4 --|
                                      |--> n5 --> n6
     */
    Dag expected = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6")));
    Assert.assertEquals(expected, fullDag.subsetAround("n2", ImmutableSet.<String>of(), ImmutableSet.<String>of()));
    Assert.assertEquals(expected, fullDag.subsetAround("n1", ImmutableSet.<String>of(), ImmutableSet.<String>of()));

    /*
         n1 --> n2 --|
                     |--> n3 --> n4 --|
         n7 --> n8 --|                |--> n5 --> n6
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6"),
      new Connection("n7", "n8"),
      new Connection("n8", "n3")));
    Assert.assertEquals(expected, fullDag.subsetAround("n3", ImmutableSet.<String>of(), ImmutableSet.<String>of()));
    Assert.assertEquals(expected, fullDag.subsetAround("n4", ImmutableSet.<String>of(), ImmutableSet.<String>of()));

    Assert.assertEquals(fullDag, fullDag.subsetAround("n5", ImmutableSet.<String>of(), ImmutableSet.<String>of()));
    Assert.assertEquals(fullDag, fullDag.subsetAround("n6", ImmutableSet.<String>of(), ImmutableSet.<String>of()));

    /*
                     |--> n3 --> n4 --|
         n7 --> n8 --|                |--> n5 --> n6
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6"),
      new Connection("n7", "n8"),
      new Connection("n8", "n3")));
    Assert.assertEquals(expected, fullDag.subsetAround("n7", ImmutableSet.<String>of(), ImmutableSet.<String>of()));
    Assert.assertEquals(expected, fullDag.subsetAround("n8", ImmutableSet.<String>of(), ImmutableSet.<String>of()));

    /*
                                      |--> n5 --> n6
                                      |
         n9 --------------------------|
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n5", "n6"),
      new Connection("n9", "n5")));
    Assert.assertEquals(expected, fullDag.subsetAround("n9", ImmutableSet.<String>of(), ImmutableSet.<String>of()));

    // test with stop nodes

    /*
         n1 --> n2 --|
                     |--> n3 --> n4
                n8 --|
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n8", "n3")));
    Assert.assertEquals(expected, fullDag.subsetAround("n3", ImmutableSet.of("n4"), ImmutableSet.of("n1", "n8")));

    /*
         n1 --> n2 --|
                     |--> n3 --> n4
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4")));
    Assert.assertEquals(expected, fullDag.subsetAround("n2", ImmutableSet.of("n4"), ImmutableSet.of("n1", "n8")));
    Assert.assertEquals(expected, fullDag.subsetAround("n1", ImmutableSet.of("n4"), ImmutableSet.of("n1", "n8")));

    /*
                          n3 --> n4 --|
                                      |--> n5 --> n6
                                      |
         n9 --------------------------|
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6"),
      new Connection("n9", "n5")));
    Assert.assertEquals(expected, fullDag.subsetAround("n5", ImmutableSet.of("n6"), ImmutableSet.of("n3", "n9")));
    Assert.assertEquals(expected, fullDag.subsetAround("n6", ImmutableSet.of("n6"), ImmutableSet.of("n3", "n9")));

    /*
                n2 --|
                     |--> n3 --> n4 --|
                n8 --|                |--> n5
                                      |
         n9 --------------------------|
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6"),
      new Connection("n8", "n3"),
      new Connection("n9", "n5")));
    Assert.assertEquals(expected, fullDag.subsetAround("n5", ImmutableSet.of("n6"), ImmutableSet.of("n2", "n8")));

    /*
                n2 --|
                     |--> n3 --> n4 --|
                n8 --|                |--> n5
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n8", "n3")));
    Assert.assertEquals(expected, fullDag.subsetAround("n4", ImmutableSet.of("n5"), ImmutableSet.of("n2", "n8")));
  }

  @Test
  public void testSubset() {
    /*
        n1 -- n2
              |
              v
        n3 -- n4 --- n8
              ^
              |
        n5-------- n6 -- n7
     */
    Dag fulldag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n4"),
      new Connection("n3", "n4"),
      new Connection("n4", "n8"),
      new Connection("n5", "n4"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7")));

    Dag expected = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n4"),
      new Connection("n4", "n8")));
    Dag actual = fulldag.subsetFrom("n1");
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n2", "n4"),
      new Connection("n4", "n8")));
    actual = fulldag.subsetFrom("n2");
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n3", "n4"),
      new Connection("n4", "n8")));
    actual = fulldag.subsetFrom("n3");
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n4", "n8"),
      new Connection("n5", "n4"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7")));
    actual = fulldag.subsetFrom("n5");
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n6", "n7")));
    actual = fulldag.subsetFrom("n6");
    Assert.assertEquals(expected, actual);

    // test subsets with stop nodes
    expected = new Dag(ImmutableSet.of(new Connection("n1", "n2")));
    actual = fulldag.subsetFrom("n1", ImmutableSet.of("n2"));
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n5", "n4"),
      new Connection("n5", "n6")));
    actual = fulldag.subsetFrom("n5", ImmutableSet.of("n4", "n6"));
    Assert.assertEquals(expected, actual);


    /*
             |--- n2 ----------|
             |                 |                              |-- n10
        n1 --|--- n3 --- n5 ---|--- n6 --- n7 --- n8 --- n9 --|
             |                 |                              |-- n11
             |--- n4 ----------|

     */
    fulldag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3"),
      new Connection("n1", "n4"),
      new Connection("n2", "n6"),
      new Connection("n3", "n5"),
      new Connection("n4", "n6"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7"),
      new Connection("n7", "n8"),
      new Connection("n8", "n9"),
      new Connection("n9", "n10"),
      new Connection("n9", "n11")));

    expected = new Dag(ImmutableSet.of(
      new Connection("n3", "n5"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7"),
      new Connection("n7", "n8"),
      new Connection("n8", "n9")));
    actual = fulldag.subsetFrom("n3", ImmutableSet.of("n4", "n9"));
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n2", "n6"),
      new Connection("n6", "n7"),
      new Connection("n7", "n8")));
    actual = fulldag.subsetFrom("n2", ImmutableSet.of("n4", "n8", "n1"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSplitByControlNodes() {

    // In following test cases note that Action nodes are named as (a0, a1...) and condition nodes are named
    // as (c0, c1, ..)


    // Test condition in the beginning and one branch connects to the action.

    // c1 --> a1 --> n1 --> n2
    //  |
    //  | --> n3 --> n4 --> a2

    Dag dag = new Dag(ImmutableSet.of(
      new Connection("c1", "a1"),
      new Connection("a1", "n1"),
      new Connection("n1", "n2"),
      new Connection("c1", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "a2")));

    Set<Dag> actual = dag.splitByControlNodes(ImmutableSet.of("c1"), ImmutableSet.of("a1", "a2"));
    Set<Dag> expectedDags = new HashSet<>();
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "a1"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("a1", "n1"), new Connection("n1", "n2"))));
    expectedDags.add(
      new Dag(ImmutableSet.of(new Connection("c1", "n3"), new Connection("n3", "n4"), new Connection("n4", "a2"))));

    Assert.assertEquals(expectedDags, actual);

    // Test condition in the end and branches connects to the Action.
    // n0-->n1--c0-->n2-->c1-->a1
    //                    |
    //                    |-->a2
    dag = new Dag(ImmutableSet.of(
      new Connection("n0", "n1"),
      new Connection("n1", "c0"),
      new Connection("c0", "n2"),
      new Connection("n2", "c1"),
      new Connection("c1", "a1"),
      new Connection("c1", "a2")));

    actual = dag.splitByControlNodes(ImmutableSet.of("c0", "c1"), ImmutableSet.of("a1", "a2"));
    expectedDags.clear();
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("n0", "n1"),
                                             new Connection("n1", "c0"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c0", "n2"), new Connection("n2", "c1"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "a2"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "a1"))));

    Assert.assertEquals(expectedDags, actual);

    // Test Actions in the beginning and connects to the Condition.
    // a1 - a2 - c1 - n0 - n1
    //      |
    // a0 --

    dag = new Dag(ImmutableSet.of(
      new Connection("a0", "a2"),
      new Connection("a1", "a2"),
      new Connection("a2", "c1"),
      new Connection("c1", "n0"),
      new Connection("n0", "n1")));

    actual = dag.splitByControlNodes(ImmutableSet.of("c1"), ImmutableSet.of("a0", "a1", "a2"));
    expectedDags.clear();
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("a0", "a2"), new Connection("a1", "a2"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("a2", "c1"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "n0"), new Connection("n0", "n1"))));
    Assert.assertEquals(expectedDags, actual);

    // Tests Actions in the beginning and connect to the Condition through other plugin
    // a1 - n0 - c1 - n1
    //      |
    // a0 --

    dag = new Dag(ImmutableSet.of(
      new Connection("a0", "n0"),
      new Connection("a1", "n0"),
      new Connection("n0", "c1"),
      new Connection("c1", "n1")));

    actual = dag.splitByControlNodes(ImmutableSet.of("c1"), ImmutableSet.of("a0", "a1"));

    expectedDags.clear();
    expectedDags.add(new Dag(
      ImmutableSet.of(new Connection("a0", "n0"), new Connection("a1", "n0"), new Connection("n0", "c1"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "n1"))));
    Assert.assertEquals(expectedDags, actual);
  }

  @Test
  public void testIdentitySplitByControl() {
    //      |-- n0 --|
    // a0 --|        |-- n2
    //      |-- n1 --|
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("a0", "n0"),
      new Connection("a0", "n1"),
      new Connection("n0", "n2"),
      new Connection("n1", "n2")));

    Set<Dag> actual = dag.splitByControlNodes(ImmutableSet.<String>of(), ImmutableSet.of("a0"));
    Set<Dag> expectedDags = new HashSet<>();
    expectedDags.add(dag);
    Assert.assertEquals(expectedDags, actual);


    // a0 -- n0 --|
    //            |-- n2
    //       n1 --|
    dag = new Dag(ImmutableSet.of(
      new Connection("a0", "n0"),
      new Connection("n0", "n2"),
      new Connection("n1", "n2")));

    actual = dag.splitByControlNodes(ImmutableSet.<String>of(), ImmutableSet.of("a0"));
    expectedDags.clear();
    expectedDags.add(dag);
    Assert.assertEquals(expectedDags, actual);

    // a0 -- n0 -- a1
    dag = new Dag(ImmutableSet.of(
      new Connection("a0", "n0"),
      new Connection("n0", "a1")));

    actual = dag.splitByControlNodes(ImmutableSet.<String>of(), ImmutableSet.of("a0", "a1"));
    expectedDags.clear();
    expectedDags.add(dag);
    Assert.assertEquals(expectedDags, actual);

    // n0 --|
    //      |-- a0
    //  |---|
    // n1
    //  |---|
    //      |-- n2
    // a1 --|
    dag = new Dag(ImmutableSet.of(
      new Connection("n0", "a0"),
      new Connection("n1", "a0"),
      new Connection("n1", "n2"),
      new Connection("a1", "n2")));

    actual = dag.splitByControlNodes(ImmutableSet.<String>of(), ImmutableSet.of("a0", "a1"));
    expectedDags.clear();
    expectedDags.add(dag);
    Assert.assertEquals(expectedDags, actual);
  }

  @Test
  public void testConditionsOnBranches() {
    /*
                        |--> n2
              |--> c1 --|
         n1 --|         |--> n3
              |
              |                |--> n5
              |--> n4 --> c2 --|
                               |--> n6
     */

    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "c1"),
      new Connection("n1", "n4"),
      new Connection("c1", "n2"),
      new Connection("c1", "n3"),
      new Connection("n4", "c2"),
      new Connection("c2", "n5"),
      new Connection("c2", "n6")));
    Set<Dag> actual = dag.splitByControlNodes(ImmutableSet.of("c1", "c2"), Collections.<String>emptySet());

    Set<Dag> expected = ImmutableSet.of(
      new Dag(ImmutableSet.of(new Connection("n1", "n4"), new Connection("n1", "c1"), new Connection("n4", "c2"))),
      new Dag(ImmutableSet.of(new Connection("c1", "n2"))),
      new Dag(ImmutableSet.of(new Connection("c1", "n3"))),
      new Dag(ImmutableSet.of(new Connection("c2", "n5"))),
      new Dag(ImmutableSet.of(new Connection("c2", "n6"))));

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testComplicatedSplitByControl() {
    /*
                                                   |-- n2 -- a3
            |-- a1 --|        |-- n0 -- n1 -- c1 --|                          |-- a5 --|
        a0--|        |-- c0 --|                    |-- n3 -- c2 -- n8 -- a4 --|        |-- a7
            |-- a2 --|        |                                               |-- a6 --|
                              |        |-- n4 -- n5 -- c4 -- c5 -- n9
                              |-- c3 --|
                                       |              |-- a8
                                       |-- n6 -- n7 --|
                                                      |-- a9
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("a0", "a1"),
      new Connection("a0", "a2"),
      new Connection("a1", "c0"),
      new Connection("a2", "c0"),
      new Connection("c0", "n0"),
      new Connection("c0", "c3"),
      new Connection("n0", "n1"),
      new Connection("n1", "c1"),
      new Connection("c1", "n2"),
      new Connection("c1", "n3"),
      new Connection("n2", "a3"),
      new Connection("n3", "c2"),
      new Connection("c2", "n8"),
      new Connection("n8", "a4"),
      new Connection("a4", "a5"),
      new Connection("a4", "a6"),
      new Connection("a5", "a7"),
      new Connection("a6", "a7"),
      new Connection("c3", "n4"),
      new Connection("c3", "n6"),
      new Connection("n4", "n5"),
      new Connection("n5", "c4"),
      new Connection("c4", "c5"),
      new Connection("c5", "n9"),
      new Connection("n6", "n7"),
      new Connection("n7", "a8"),
      new Connection("n7", "a9")));

    Set<Dag> actual = dag.splitByControlNodes(
      ImmutableSet.of("c0", "c1", "c2", "c3", "c4", "c5"),
      ImmutableSet.of("a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"));

    Set<Dag> expected = ImmutableSet.of(
      new Dag(ImmutableSet.of(new Connection("a0", "a1"), new Connection("a0", "a2"))),
      new Dag(ImmutableSet.of(new Connection("a1", "c0"), new Connection("a2", "c0"))),
      new Dag(ImmutableSet.of(new Connection("c0", "n0"), new Connection("n0", "n1"), new Connection("n1", "c1"))),
      new Dag(ImmutableSet.of(new Connection("c0", "c3"))),
      new Dag(ImmutableSet.of(new Connection("c1", "n2"), new Connection("n2", "a3"))),
      new Dag(ImmutableSet.of(new Connection("c1", "n3"), new Connection("n3", "c2"))),
      new Dag(ImmutableSet.of(new Connection("c2", "n8"), new Connection("n8", "a4"))),
      new Dag(ImmutableSet.of(new Connection("a4", "a5"), new Connection("a4", "a6"))),
      new Dag(ImmutableSet.of(new Connection("a5", "a7"), new Connection("a6", "a7"))),
      new Dag(ImmutableSet.of(new Connection("c3", "n4"), new Connection("n4", "n5"), new Connection("n5", "c4"))),
      new Dag(ImmutableSet.of(new Connection("c3", "n6"), new Connection("n6", "n7"),
                              new Connection("n7", "a8"), new Connection("n7", "a9"))),
      new Dag(ImmutableSet.of(new Connection("c4", "c5"))),
      new Dag(ImmutableSet.of(new Connection("c5", "n9"))));

    Assert.assertEquals(expected, actual);
  }
}
