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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.Predicate;
import io.cdap.cdap.etl.proto.Connection;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;

/**
 * A DAG (directed acyclic graph).
 */
public class Dag implements Serializable {

  protected final Set<String> nodes;
  protected final Set<String> sources;
  protected final Set<String> sinks;
  // stage -> outputs of that stage
  protected final SetMultimap<String, String> outgoingConnections;
  // stage -> inputs for that stage
  protected final SetMultimap<String, String> incomingConnections;

  public Dag(Collection<Connection> connections) {
    Preconditions.checkArgument(!connections.isEmpty(),
        "Cannot create a DAG without any connections");
    this.outgoingConnections = HashMultimap.create();
    this.incomingConnections = HashMultimap.create();
    for (Connection connection : connections) {
      outgoingConnections.put(connection.getFrom(), connection.getTo());
      incomingConnections.put(connection.getTo(), connection.getFrom());
    }
    this.sources = new HashSet<>();
    this.sinks = new HashSet<>();
    this.nodes = new HashSet<>();
    init();
    validate();
  }

  protected Dag(SetMultimap<String, String> outgoingConnections,
      SetMultimap<String, String> incomingConnections) {
    this.outgoingConnections = HashMultimap.create(outgoingConnections);
    this.incomingConnections = HashMultimap.create(incomingConnections);
    this.sources = new HashSet<>();
    this.sinks = new HashSet<>();
    this.nodes = new HashSet<>();
    init();
  }

  protected Dag(Dag other) {
    this(other.outgoingConnections, other.incomingConnections);
  }

  /**
   * Validate the DAG is a valid DAG without cycles, and no islands. This should only be called
   * before any mutating operations like {@link #removeSource()} are called.
   *
   * @throws IllegalStateException if there is a cycle in the graph, or an island in the graph
   */
  void validate() {
    // if there are no sources, we must have a cycle.
    if (sources.isEmpty()) {
      throw new IllegalStateException(
          "DAG does not have any sources. Please remove cycles from the graph.");
    }
    // similarly, if there are no sinks, we must have a cycle
    if (sinks.isEmpty()) {
      throw new IllegalStateException(
          "DAG does not have any sinks. Please remove cycles from the graph.");
    }

    // check for cycles
    getTopologicalOrder();

    // check for sections of the dag that are on an island by themselves

    // source -> [ nodes accessible by the source ]
    Map<String, Set<String>> nodesAccessibleBySources = new HashMap<>();
    for (String source : sources) {
      nodesAccessibleBySources.put(source, accessibleFrom(source));
    }

    // the algorithm is to keep an island and try to keep adding to it until we can't anymore.
    // the island starts off as the nodes accessible by the first source
    // we then loop through all other sources and add them to the island if they can access any node in the island.
    // we stop if we ever loop through all other sources and can't add them to the island,
    // or if the island has grown to include all sources.
    Set<String> islandNodes = new HashSet<>();
    // seed the island with the first source
    Set<String> potentialIslandSources = new HashSet<>(sources);
    String firstSource = potentialIslandSources.iterator().next();
    islandNodes.addAll(nodesAccessibleBySources.get(firstSource));
    potentialIslandSources.remove(firstSource);

    while (!potentialIslandSources.isEmpty()) {
      Set<String> sourcesAdded = new HashSet<>();
      // for each source not yet a part of the island
      for (String potentialIslandSource : potentialIslandSources) {
        Set<String> accessibleBySource = nodesAccessibleBySources.get(potentialIslandSource);
        // if that source can access the island in any way, add it to the island
        if (!Sets.intersection(islandNodes, accessibleBySource).isEmpty()) {
          islandNodes.addAll(accessibleBySource);
          sourcesAdded.add(potentialIslandSource);
        }
      }
      // if this is empty, no sources were added to the island. That means the island really is an island.
      if (sourcesAdded.isEmpty()) {
        throw new DisjointConnectionsException(
            String.format(
                "Invalid DAG. There is an island made up of stages %s (no other stages connect to them).",
                Joiner.on(',').join(islandNodes)));
      }
      potentialIslandSources.removeAll(sourcesAdded);
    }
  }

  /**
   * Split the dag based on the input control nodes. Control nodes represent actions or conditions.
   * Actions can only be at the front or end of the dag. Condition nodes can be anywhere in the dag,
   * but can only have at most 2 output branches. Output branches must never intersect.
   *
   * This method will break up the dag into a set of interconnected subdags, where control nodes are
   * guaranteed to be either a source or a sink in a subdag, but not somewhere in the middle. There
   * will be a subdag for each branch of a condition. All connected non-control nodes are guaranteed
   * to be placed in the same subdag. Subdags are connected if a sink in one subdag is a source in
   * another subdag.
   *
   * For example: |-- action1 |-- n0 --|                |-- n2 -- n3 --| action0 --|        |--
   * condition0 --|              |-- action2 |-- n1 --|                |-- n4
   *
   * will be split into: |-- action1 |-- n0 --|                  condition0 -- n2 -- n3 --| action0
   * --|        |-- condition0                              |-- action2 |-- n1 --| condition0 -- n4
   *
   * @param conditionNodes condition nodes
   * @param actionNodes action nodes
   * @return set of splitted dags
   */
  public Set<Dag> splitByControlNodes(Set<String> conditionNodes, Set<String> actionNodes) {

    Set<Dag> dags = new HashSet<>();
    Set<String> controlNodes = Sets.union(conditionNodes, actionNodes);

    /*
        Consider the following example, where c1, c2, and c3 are conditions:

                       c1 - t1---agg1--agg2---sink1
                        |
                        ----c2 - sink2
                             |
                              ------c3 - sink3

        This next part would add subdags:

        5. c1 - t1 - agg1 - agg2 - sink1
        6. c1 - c2
        7. c2 - sink2
        8. c2 - c3
        9. c3 - sink3
     */
    for (String conditionNode : conditionNodes) {
      for (String output : getNodeOutputs(conditionNode)) {
        if (controlNodes.contains(output)) {
          dags.add(createSubDag(ImmutableSet.of(conditionNode, output)));
        } else {
          Set<String> branch = accessibleFrom(output, controlNodes);
          branch.add(conditionNode);
          dags.add(createSubDag(branch));
        }
      }
    }

    Set<String> nonControlSources = Sets.difference(getSources(), conditionNodes);
    for (String actionNode : Sets.union(actionNodes, nonControlSources)) {
      Set<String> accessibleFrom = accessibleFrom(actionNode, controlNodes);
      // if it's just one node, that means its a sink and was already included in another subdag, so we can skip it
      if (accessibleFrom.size() < 2) {
        continue;
      }
      Dag subdag = createSubDag(accessibleFrom);
      /*
          Actions are different from conditions in that the branches of an action may eventually intersect
          Consider the following dag:

                 |-- n0 --|
            a0 --|        |--|
                 |-- n1 --|  |
                             |-- n2
                             |
            a1 -- n3 --------|

          If 'actionNode' = 'a0', then subdag is:

                 |-- n0 --|
            a0 --|        |-- n2
                 |-- n1 --|

          But this subdag is incomplete, as it does not preserve all connections between non-control nodes (n3 -> n2).
          To fix this, while there are new nodes being added to subdag:
            For each new sink, add all parents of the sink
            For each new source, add all children of the source
      */
      Set<String> newSinks = new HashSet<>(subdag.getSinks());
      while (!newSinks.isEmpty()) {
        Set<String> oldSources = new HashSet<>(subdag.getSources());
        Set<String> nodes = new HashSet<>(subdag.getNodes());
        for (String newSink : newSinks) {
          nodes.addAll(parentsOf(newSink, controlNodes));
        }
        subdag = createSubDag(nodes);

        Set<String> oldSinks = new HashSet<>(subdag.getSinks());
        Set<String> newSources = Sets.difference(subdag.getSources(), oldSources);
        for (String newSource : newSources) {
          nodes.addAll(accessibleFrom(newSource, controlNodes));
        }
        subdag = createSubDag(nodes);
        newSinks = Sets.difference(subdag.getSinks(), oldSinks);
      }
      dags.add(subdag);
    }

    return dags;
  }

  public Set<String> getNodes() {
    return nodes;
  }

  public Set<String> getSources() {
    return Collections.unmodifiableSet(new TreeSet<>(sources));
  }

  public Set<String> getSinks() {
    return Collections.unmodifiableSet(new TreeSet<>(sinks));
  }

  public Set<String> getNodeOutputs(String node) {
    return Collections.unmodifiableSet(outgoingConnections.get(node));
  }

  public Set<String> getNodeInputs(String node) {
    return Collections.unmodifiableSet(incomingConnections.get(node));
  }


  /**
   * Return all stages accessible from a starting stage.
   *
   * @param stage the stage to start at
   * @return all stages accessible from that stage
   */
  public Set<String> accessibleFrom(String stage) {
    return accessibleFrom(stage, ImmutableSet.<String>of());
  }

  /**
   * Return all stages accessible from a starting stage, without going past any node in stopNodes.
   * The starting stage is not treated as a stop node, even if it is in the stop nodes set.
   *
   * @param stage the stage to start at
   * @param stopNodes set of nodes to stop traversal on
   * @return all stages accessible from that stage
   */
  public Set<String> accessibleFrom(String stage, Set<String> stopNodes) {
    return accessibleFrom(ImmutableSet.of(stage), stopNodes);
  }

  /**
   * Return all stages accessible from the starting stages, without going past any node in
   * stopNodes. Starting stages are not treated as stop nodes, even if they are in the stop nodes
   * set.
   *
   * @param stages the stages to start at
   * @param stopNodes set of nodes to stop traversal on
   * @return all stages accessible from that stage
   */
  public Set<String> accessibleFrom(Set<String> stages, Set<String> stopNodes) {
    Set<String> accessible = new HashSet<>();
    final Set<String> nonStartingStopNodes = Sets.difference(stopNodes, stages);
    for (String stage : stages) {
      traverseForwards(stage, accessible, new StopNodeCondition(nonStartingStopNodes));
    }
    return accessible;
  }

  /**
   * Return all stages that are parents of an ending stage. Stage X is a parent of stage Y if there
   * is a path from X to Y.
   *
   * @param stage the stage to start at
   * @return all parents of that stage
   */
  public Set<String> parentsOf(String stage) {
    return parentsOf(stage, ImmutableSet.<String>of());
  }

  /**
   * Return all stages that are parents of an ending stage, without going past any node in
   * stopNodes. A stage counts as a parent of itself. The starting stage is not counted as a stop
   * node, even if it is in the set of stop nodes.
   *
   * @param stage the stage to start at
   * @param stopNodes set of nodes to stop traversal on
   * @return all parents of that stage
   */
  public Set<String> parentsOf(String stage, final Set<String> stopNodes) {
    Set<String> accessible = new HashSet<>();
    final Set<String> nonStartingStopNodes = new HashSet<>(stopNodes);
    nonStartingStopNodes.remove(stage);
    traverseBackwards(stage, accessible, new StopNodeCondition(nonStartingStopNodes));
    return accessible;
  }

  /**
   * Return a subset of this dag starting from the specified stage. This is equivalent to calling
   * {@link #subsetFrom(String, Set)} with an empty set for stop nodes
   *
   * @param stage the stage to start at
   * @return a dag created from the nodes accessible from the specified stage
   */
  public Dag subsetFrom(String stage) {
    return subsetFrom(stage, ImmutableSet.<String>of());
  }

  /**
   * Return a subset of this dag starting from the specified stage, without going past any node in
   * stopNodes. This is equivalent to taking the nodes from {@link #accessibleFrom(String, Set)} and
   * building a dag from them.
   *
   * @param stage the stage to start at
   * @param stopNodes set of nodes to stop traversal on
   * @return a dag created from the nodes accessible from the specified stage
   */
  public Dag subsetFrom(String stage, Set<String> stopNodes) {
    return subsetFrom(ImmutableSet.of(stage), stopNodes);
  }

  /**
   * Return a subset of this dag starting from the specified stages. This is equivalent to calling
   * {@link #subsetFrom(Set, Set)} with an empty set for stop nodes
   *
   * @param stages the stage to start at
   * @return a dag created from the nodes accessible from the specified stage
   */
  public Dag subsetFrom(Set<String> stages) {
    return subsetFrom(stages, ImmutableSet.<String>of());
  }

  /**
   * Return a subset of this dag starting from the specified stage, without going past any node in
   * stopNodes. This is equivalent to taking the nodes from {@link #accessibleFrom(Set, Set)} and
   * building a dag from them.
   *
   * @param stages the stages to start at
   * @param stopNodes set of nodes to stop traversal on
   * @return a dag created from the nodes accessible from the specified stage
   */
  public Dag subsetFrom(Set<String> stages, Set<String> stopNodes) {
    Set<String> nodes = accessibleFrom(stages, stopNodes);
    Set<Connection> connections = new HashSet<>();
    for (String node : nodes) {
      for (String outputNode : outgoingConnections.get(node)) {
        if (nodes.contains(outputNode)) {
          connections.add(new Connection(node, outputNode));
        }
      }
    }
    return new Dag(connections);
  }

  /**
   * Return a subset of this dag starting from the specified stage, without going past any node in
   * the child stop nodes and parent stop nodes. If the parent or child stop nodes contain the
   * starting stage, it will be ignored. This is equivalent to taking the nodes from {@link
   * #accessibleFrom(Set, Set)}, {@link #parentsOf(String, Set)}, and building a dag from them.
   *
   * @param stage the stage to start at
   * @param childStopNodes set of nodes to stop traversing forwards on
   * @param parentStopNodes set of nodes to stop traversing backwards on
   * @return a dag created from the stages given and child nodes of those stages and parent nodes of
   *     those stages.
   */
  public Dag subsetAround(String stage, Set<String> childStopNodes, Set<String> parentStopNodes) {
    Set<String> nodes = Sets.union(accessibleFrom(stage, childStopNodes),
        parentsOf(stage, parentStopNodes));
    Set<Connection> connections = new HashSet<>();
    for (String node : nodes) {
      for (String outputNode : outgoingConnections.get(node)) {
        if (nodes.contains(outputNode)) {
          connections.add(new Connection(node, outputNode));
        }
      }
    }
    return new Dag(connections);
  }

  /**
   * Creates dag from provided list of nodes
   *
   * @param nodes list of nodes to create subdag
   * @return Dag with connections among nodes
   */
  public Dag createSubDag(Set<String> nodes) {
    Set<Connection> connections = new HashSet<>();
    for (String node : nodes) {
      for (String outputNode : outgoingConnections.get(node)) {
        if (nodes.contains(outputNode)) {
          connections.add(new Connection(node, outputNode));
        }
      }
    }
    return new Dag(connections);
  }

  /**
   * Get the dag in topological order. The returned list guarantees that for each item in the list,
   * that item has no path to an item that comes before it in the list. In the process, if a cycle
   * is found, an exception will be thrown. Topological sort means we pop off a source from the dag,
   * re-calculate sources, and continue until there are no more nodes left. Popping will be done on
   * a copy of the dag so that this is not a destructive operation.
   *
   * @return the dag in topological order
   * @throws IllegalStateException if there is a cycle in the dag
   */
  public List<String> getTopologicalOrder() {
    List<String> linearized = new ArrayList<>();

    Dag copy = new Dag(outgoingConnections, incomingConnections);
    String removed;
    while ((removed = copy.removeSource()) != null) {
      linearized.add(removed);
    }
    if (copy.outgoingConnections.isEmpty()) {
      return linearized;
    }
    // if we've run out of sources to remove, but there are still connections, that means there is a cycle.
    // remove all sinks so we can print out where the cycle is.
    do {
      removed = copy.removeSink();
    } while (removed != null);
    Set<String> cycle = accessibleFrom(copy.outgoingConnections.keySet().iterator().next());
    throw new IllegalStateException(
        String.format("Invalid DAG. Stages %s form a cycle.", Joiner.on(',').join(cycle)));
  }

  /**
   * Get the branch the given node is on, in the order the nodes appear on the branch, ending with
   * the given node. Every node returned has exactly one input except for the first node, which can
   * have any number of inputs. Every node returned has exactly one output except for the last node,
   * which can have any number of outputs.
   *
   * @param node the branch node
   * @param stopNodes any nodes to stop on
   * @return the branch the node is on
   */
  public List<String> getBranch(final String node, final Set<String> stopNodes) {
    List<String> branchNodes = new ArrayList<>();
    traverse(node, branchNodes, incomingConnections, new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        // stop if we hit a stop node
        if (stopNodes.contains(input) && !node.equals(input)) {
          return true;
        }
        // stop if there are multiple inputs or no inputs
        Set<String> inputs = incomingConnections.get(input);
        if (inputs.size() != 1) {
          return true;
        }
        // stop if the input has multiple outputs.
        String inputNode = inputs.iterator().next();
        return outgoingConnections.get(inputNode).size() > 1;
      }
    });
    Collections.reverse(branchNodes);
    return branchNodes;
  }

  /**
   * Traverse the dag starting at the specified node and stopping when a node has no outputs, when
   * it has already been visited, or when it meets the specified stop condition. All nodes that were
   * visited will be added to the specified visitedNodes collection in the order that they are
   * traversed. The stop condition will be applied to the starting node as well.
   *
   * @param node the node to start at
   * @param visitedNodes collection to add all visited nodes to
   * @param connections map from a node to all its outputs
   * @param stopCondition condition to stop traversing
   */
  protected void traverse(String node, Collection<String> visitedNodes,
      SetMultimap<String, String> connections, Predicate<String> stopCondition) {
    if (!visitedNodes.add(node) || stopCondition.apply(node)) {
      return;
    }
    for (String output : connections.get(node)) {
      traverse(output, visitedNodes, connections, stopCondition);
    }
  }

  /**
   * Remove a source from the dag. New sources will be re-calculated after the source is removed.
   *
   * @return the removed source, or null if there were no sources to remove.
   */
  protected String removeSource() {
    if (sources.isEmpty()) {
      return null;
    }
    String source = sources.iterator().next();
    removeNode(source);
    return source;
  }

  /**
   * Remove the specified connection. Does not check that the connection actually exists. It is
   * possible to break apart the dag with this call.
   */
  protected void removeConnection(String from, String to) {
    outgoingConnections.remove(from, to);
    incomingConnections.remove(to, from);
  }

  /**
   * Add the specified connection. It is possible to create a cycle with this call.
   */
  protected void addConnection(String from, String to) {
    outgoingConnections.put(from, to);
    incomingConnections.put(to, from);
  }

  /**
   * Remove a specific node from the dag. Removing a node will remove all connections into the node
   * and all connection coming out of the node. Removing a node will also re-compute the sources and
   * sinks of the dag.
   *
   * @param node the node to remove
   */
  protected void removeNode(String node) {
    // for each node this output to: node -> outputNode
    for (String outputNode : outgoingConnections.removeAll(node)) {
      // remove the connection from this node to its output
      incomingConnections.remove(outputNode, node);
      // if the removal of that connection caused the output to become a source, add it as a source
      if (incomingConnections.get(outputNode).isEmpty()) {
        sources.add(outputNode);
      }
    }
    // for each node that output to this node: inputNode -> node
    for (String inputNode : incomingConnections.removeAll(node)) {
      // remove the connection from the input node to this node
      outgoingConnections.remove(inputNode, node);
      // if the removal of that connection caused the input node to become a sink, add it as a sink
      if (outgoingConnections.get(inputNode).isEmpty()) {
        sinks.add(inputNode);
      }
    }
    // in case this node we removed a source or a sink (or both).
    sinks.remove(node);
    sources.remove(node);
    nodes.remove(node);
  }

  private void traverseForwards(String node, Collection<String> visitedNodes,
      Predicate<String> stopCondition) {
    traverse(node, visitedNodes, outgoingConnections, stopCondition);
  }

  private void traverseBackwards(String node, Collection<String> visitedNodes,
      Predicate<String> stopCondition) {
    traverse(node, visitedNodes, incomingConnections, stopCondition);
  }

  private String removeSink() {
    if (sinks.isEmpty()) {
      return null;
    }
    String sink = sinks.iterator().next();
    removeNode(sink);
    return sink;
  }

  private void init() {
    nodes.clear();
    sources.clear();
    sinks.clear();
    nodes.addAll(outgoingConnections.keySet());
    nodes.addAll(outgoingConnections.values());
    for (String node : nodes) {
      if (outgoingConnections.get(node).isEmpty()) {
        sinks.add(node);
      }
      if (incomingConnections.get(node).isEmpty()) {
        sources.add(node);
      }
    }
  }

  /**
   * Returns true if an input is in a set of stop nodes.
   */
  private static class StopNodeCondition implements Predicate<String> {

    private final Set<String> stopNodes;

    private StopNodeCondition(Set<String> stopNodes) {
      this.stopNodes = stopNodes;
    }

    @Override
    public boolean apply(@Nullable String input) {
      return stopNodes.contains(input);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Dag that = (Dag) o;

    return Objects.equals(nodes, that.nodes)
        && Objects.equals(sources, that.sources)
        && Objects.equals(sinks, that.sinks)
        && Objects.equals(outgoingConnections, that.outgoingConnections)
        && Objects.equals(incomingConnections, that.incomingConnections);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodes, sources, sinks, outgoingConnections, incomingConnections);
  }

  @Override
  public String toString() {
    return "Dag{"
        + "nodes=" + nodes
        + ", sources=" + sources
        + ", sinks=" + sinks
        + ", outgoingConnections=" + outgoingConnections
        + ", incomingConnections=" + incomingConnections
        + '}';
  }
}
