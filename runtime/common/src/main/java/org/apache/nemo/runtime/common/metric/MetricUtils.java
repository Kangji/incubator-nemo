/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nemo.runtime.common.metric;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * Utility class for metrics.
 */
public final class MetricUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class.getName());

  static {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      throw new MetricException("PostgreSQL Driver not found: " + e);
    }
  }

  public static final String SQLITE_DB_NAME =
    "jdbc:sqlite:" + Util.fetchProjectRootPath() + "/optimization_db.sqlite3";
  public static final String POSTGRESQL_DB_NAME =
    "jdbc:postgresql://nemo-optimization.cabbufr3evny.us-west-2.rds.amazonaws.com:5432/nemo_optimization";

  /**
   * Private constructor.
   */
  private MetricUtils() {
  }

  /**
   * Stringify execution properties of an IR DAG.
   *
   * @param irdag the IR DAG to observe.
   * @return stringified execution properties, grouped as patterns. Left is for vertices, right is for edges.
   */
  public static String stringifyIRDAGProperties(final IRDAG irdag) {
    return stringifyIRDAGProperties(irdag, 0);  // pattern recording is default
  }

  /**
   * Stringify execution properties of an IR DAG.
   *
   * @param irdag IR DAG to observe.
   * @param mode 0: record metrics by patterns, 1: record metrics with vertex or edge IDs.
   * @return the pair of stringified execution properties. Left is for vertices, right is for edges.
   */
  static String stringifyIRDAGProperties(final IRDAG irdag, final Integer mode) {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode node = mapper.createObjectNode();
    final ArrayNode verticesNode = mapper.createArrayNode();
    final ArrayNode edgesNode = mapper.createArrayNode();

    node.put("inputsize", irdag.getInputSize());
    node.put("jvmmemsize", Runtime.getRuntime().maxMemory());
    node.put("totalmemsize", ((com.sun.management.OperatingSystemMXBean) ManagementFactory
      .getOperatingSystemMXBean()).getTotalPhysicalMemorySize());
    node.put("dagsummary", irdag.irDAGSummary());

    if (mode == 0) {  // Patterns
      node.put("type", "pattern");
      LOG.info("Vertices list: {}", irdag.getTopologicalSort());
      for (final IRVertex v: irdag.getTopologicalSort()) {
        final List<IREdge> incomingEdges = irdag.getIncomingEdgesOf(v);

        final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
        builder.addVertex(v);
        incomingEdges.forEach(e ->
          builder.addVertex(e.getSrc()).connectVertices(e));
        final String subDAGString = subDAGToString(new IRDAG(builder.buildWithoutSourceSinkCheck()));

        // Let's now add the execution properties of the given pattern.
        final AtomicInteger idx = new AtomicInteger(0);

        // The vertex itself
        final ObjectNode vertexNode = mapper.createObjectNode();
        v.getExecutionProperties().forEachProperties(ep ->
          epFormatter(vertexNode, subDAGString + "-" + idx.get(),
            ep, v.getExecutionProperties().isPropertyFinalized(ep)));
        verticesNode.add(vertexNode);
        // Main incoming edges & vertex
        idx.getAndIncrement();
        incomingEdges.stream()
          .sorted(Comparator.comparing(e -> stringifyVertex(e.getSrc())))
          .forEachOrdered(e -> {
            e.getExecutionProperties().forEachProperties(ep -> {
              final ObjectNode eNode = mapper.createObjectNode();
              epFormatter(eNode, subDAGString + "-" + idx.get(),
                ep, e.getExecutionProperties().isPropertyFinalized(ep));
              edgesNode.add(eNode);
            });
            idx.getAndIncrement();
            e.getSrc().getExecutionProperties().forEachProperties(ep -> {
              final ObjectNode vNode = mapper.createObjectNode();
              epFormatter(vNode, subDAGString + "-" + idx.get(),
                ep, e.getSrc().getExecutionProperties().isPropertyFinalized(ep));
              verticesNode.add(vNode);
            });
            idx.getAndIncrement();
          });
      }
      node.set("vertex", verticesNode);
      node.set("edge", edgesNode);
    } else if (mode == 1) {  // Learning the 'rules'.
      node.put("type", "rule");
      node.put("rules", irdag.getNamesOfRulesApplied());
    } else {  // By Vertex / Edge IDs.
      node.put("type", "id");
      irdag.getVertices().forEach(v ->
        v.getExecutionProperties().forEachProperties(ep -> {
          final ObjectNode vertexNode = mapper.createObjectNode();
          epFormatter(vertexNode, v.getId(), ep, v.getExecutionProperties().isPropertyFinalized(ep));
          verticesNode.add(vertexNode);
        }));

      irdag.getVertices().forEach(v ->
        irdag.getIncomingEdgesOf(v).forEach(e ->
          e.getExecutionProperties().forEachProperties(ep -> {
            final ObjectNode edgeNode = mapper.createObjectNode();
            epFormatter(edgeNode, e.getId(), ep, e.getExecutionProperties().isPropertyFinalized(ep));
            edgesNode.add(edgeNode);
          })));
      node.set("vertex", verticesNode);
      node.set("edge", edgesNode);
    }

    // Update the metric metadata if new execution property key / values have been discovered and updates are required.
    return node.toString();
  }

  /**
   * Formatter for execution properties. It updates the metadata for the metrics if new EP key / values are discovered.
   *
   * @param node node append the metrics to.
   * @param id   the string of the ID or the pattern of the vertex or the edge.
   * @param ep   the execution property.
   * @param isFinalized whether or not the execution property is permanent/finalized.
   */
  private static void epFormatter(final ObjectNode node, final String id,
                                  final ExecutionProperty<?> ep, final Boolean isFinalized) {
    node.put("ID", id);
    node.put("EPKeyClass", ep.getClass().getName());
    node.put("EPValueClass", ep.getValue().getClass().getName());
    final String epValueStr = epValueToString(ep);
    node.put("EPValue", epValueStr);
    node.put("isFinalized", isFinalized.toString());
  }

  /**
   * Helper method to convert Execution Property value objects to an integer index.
   * It updates the metadata for the metrics if new EP values are discovered.
   *
   * @param ep         the execution property containing the value.
   * @return the converted value index.
   */
  static String epValueToString(final ExecutionProperty<?> ep) {
    final Serializable o = ep.getValue();

    if (o instanceof Enum) {
      return String.valueOf(((Enum) o).ordinal());
    } else if (o instanceof Integer) {
      return o.toString();
    } else if (o instanceof Boolean) {
      return o.toString();
    } else {
      return new String(SerializationUtils.serialize(o), ISO_8859_1);
    }
  }

  /**
   * Stringify a vertex with the name of its class and the transform, for pattern matching.
   *
   * @param vertex the vertex to stringify.
   * @return the string containing the information of the IRVertex class and the transform (if applicable).
   */
  private static String stringifyVertex(final IRVertex vertex) {
    if (vertex instanceof OperatorVertex) {
      return vertex.getClass().getSimpleName()
        + "{" + ((OperatorVertex) vertex).getTransform().getClass().getSimpleName() + "}";
    } else {
      return vertex.getClass().getSimpleName();
    }
  }

  /**
   * @param subDAG the sub-DAG of a pattern to stringify.
   *
   * @return the stringified sub-DAG.
   */
  private static String subDAGToString(final IRDAG subDAG) {
    final StringBuilder res = new StringBuilder();
    final List<IRVertex> topologicalVertices = subDAG.getTopologicalSort();
    final IRVertex targetVertex = topologicalVertices.get(topologicalVertices.size() - 1);

    res.append(stringifyVertex(targetVertex));

    subDAG.getIncomingEdgesOf(targetVertex).stream()
      .sorted(Comparator.comparing(e -> stringifyVertex(e.getSrc())))
      .forEachOrdered(e -> {
        res.append('|').append(e.getClass().getSimpleName());
        res.append('|').append(stringifyVertex(e.getSrc()));
      });

    return res.toString();
  }

  /**
   * Searches the DAG for the objects that matches the given ID.
   * @param id the ID of the object to find.
   * @param dag the DAG to get the object from.
   *
   * @return the list of objects that matches the given ID on the DAG.
   */
  public static List<Object> getObjectFromPatternString(final String id, final IRDAG dag) {
    final String[] str = id.split("-");
    final Integer index = Integer.parseInt(str[1]);
    final String[] stringifiedElems = str[0].split("\\|");
    final List<Object> objectList = new ArrayList<>();

    dag.getVertices().stream()
      .filter(v -> stringifyVertex(v).equals(stringifiedElems[0]))
      .filter(v -> {
        final Iterator<IREdge> edges = dag.getIncomingEdgesOf(v).stream()
          .sorted(Comparator.comparing(e -> stringifyVertex(e.getSrc())))
          .iterator();
        final AtomicInteger i = new AtomicInteger(1);
        while (edges.hasNext()) {
          final IREdge e = edges.next();
          if (!e.getClass().getSimpleName().equals(stringifiedElems[i.getAndIncrement()])
            || !stringifyVertex(e.getSrc()).equals(stringifiedElems[i.getAndIncrement()])) {
            return false;
          }
        }
        return true;
      }).forEach(v -> {
        if (index == 0) {
          objectList.add(v);
        } else {
          final Iterator<IREdge> edges = dag.getIncomingEdgesOf(v).stream()
            .sorted(Comparator.comparing(e -> stringifyVertex(e.getSrc())))
            .iterator();
          final AtomicInteger i = new AtomicInteger(1);
          while (edges.hasNext()) {
            final IREdge e = edges.next();
            if (index == i.getAndIncrement()) {
              objectList.add(e);
              break;
            } else if (index == i.get()) {
              objectList.add(e.getSrc());
              break;
            } else {
              i.getAndIncrement();
            }
          }
        }
      });
    return objectList;
  }

  /**
   * Recursive method for getting the parameter type of the execution property.
   * This can be used, for example, to get DecoderFactory, instead of BeamDecoderFactory.
   *
   * @param epClass    execution property class to observe.
   * @param valueClass the value class of the execution property.
   * @return the parameter type.
   */
  private static Class<? extends Serializable> getParameterType(final Class<? extends ExecutionProperty> epClass,
                                                                final Class<? extends Serializable> valueClass) {
    if (!getMethodFor(epClass, "of", valueClass.getSuperclass()).isPresent()
      || !(Serializable.class.isAssignableFrom(valueClass.getSuperclass()))) {
      final Class<? extends Serializable> candidate = Arrays.stream(valueClass.getInterfaces())
        .filter(vc -> Serializable.class.isAssignableFrom(vc) && getMethodFor(epClass, "of", vc).isPresent())
        .map(vc -> getParameterType(epClass, ((Class<? extends Serializable>) vc))).findFirst().orElse(null);
      return candidate == null ? valueClass : candidate;
    } else {
      return getParameterType(epClass, ((Class<? extends Serializable>) valueClass.getSuperclass()));
    }
  }

  /**
   * Utility method to getting an optional method called 'name' for the class.
   *
   * @param clazz      class to get the method of.
   * @param name       the name of the method.
   * @param valueTypes the value types of the method.
   * @return optional of the method. It returns Optional.empty() if the method could not be found.
   */
  public static Optional<Method> getMethodFor(final Class<? extends ExecutionProperty> clazz,
                                              final String name, final Class<?>... valueTypes) {
    try {
      final Method mthd = clazz.getMethod(name, valueTypes);
      return Optional.of(mthd);
    } catch (final Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Helper method to do the opposite of the #epValueToString method.
   * It receives the split, and the direction of the tweak value (which show the target index value),
   * and returns the actual value which the execution property uses.
   *
   * @param epValue      the EP value, in the form of a string.
   * @param epValueClass the EP value class to retrieve information from.
   * @return the project root path.
   * @throws ClassNotFoundException exception when no class has been found for the epValueClass.
   */
  static Serializable stringToValue(final String epValue, final String epValueClass) throws ClassNotFoundException {
    final Class<? extends Serializable> targetObjectClass = (Class<? extends Serializable>) Class.forName(epValueClass);
    if (targetObjectClass.isEnum()) {
      final int value = Integer.parseInt(epValue);
      final int maxOrdinal = targetObjectClass.getFields().length - 1;
      final int ordinal = value < 0 ? 0 : (value > maxOrdinal ? maxOrdinal : value);
      LOG.info("Translated: {} into ENUM with ordinal {}", epValue, ordinal);
      return targetObjectClass.getEnumConstants()[ordinal];
    } else if (targetObjectClass.isAssignableFrom(Integer.class)) {
      final Integer value = Integer.parseInt(epValue);
      final Integer res = value < 0 ? 0 : value;
      LOG.info("Translated: {} into INTEGER of {}", epValue, res);
      return res;
    } else if (targetObjectClass.isAssignableFrom(Boolean.class)) {
      final Boolean res = Boolean.parseBoolean(epValue);
      LOG.info("Translated: {} into BOOLEAN of {}", epValue, res);
      return res;
    } else {
      final Serializable res = SerializationUtils.deserialize(epValue.getBytes(ISO_8859_1));
      LOG.info("Translated: {} into VALUE of {}", epValue, res);
      return res;
    }
  }

  /**
   * Receives the pair of execution property and value classes, and returns the optimized value of the EP.
   *
   * @param epKeyClass   the EP Key class to retrieve the new EP from.
   * @param epValueClass the EP Value class to retrieve the new EP Value of.
   * @param epValue      the execution property value, stringified or encoded to a string.
   * @return The execution property constructed from the key index and the split value.
   */
  public static ExecutionProperty<? extends Serializable> keyAndValueToEP(
    final String epKeyClass,
    final String epValueClass,
    final String epValue) {

    final ExecutionProperty<? extends Serializable> ep;
    try {
      final Class<? extends ExecutionProperty> epClass = (Class<? extends ExecutionProperty>) Class.forName(epKeyClass);
      final Serializable value = stringToValue(epValue, epValueClass);

      final Method staticConstructor = getMethodFor(epClass, "of", getParameterType(epClass, value.getClass()))
        .orElseThrow(NoSuchMethodException::new);
      ep = (ExecutionProperty<? extends Serializable>) staticConstructor.invoke(null, value);
    } catch (final NoSuchMethodException e) {
      throw new MetricException("Class " + epKeyClass
        + " does not have a static method exposing the constructor 'of' with value type " + epValueClass
        + ": " + e);
    } catch (final Exception e) {
      throw new MetricException(e);
    }
    return ep;
  }

  /**
   * Configuration space.
   */
  private static Set<String> configurationSpace = Stream.of(
    ClonedSchedulingProperty.class.getName(),
    ParallelismProperty.class.getName(),
    ResourceAntiAffinityProperty.class.getName(),
    ResourceLocalityProperty.class.getName(),
    ResourcePriorityProperty.class.getName(),
    ResourceSiteProperty.class.getName(),
    ResourceSlotProperty.class.getName(),
    ResourceTypeProperty.class.getName(),

    CompressionProperty.class.getName(),
    DataFlowProperty.class.getName(),
    DataPersistenceProperty.class.getName(),
    DataStoreProperty.class.getName(),
    PartitionerProperty.class.getName(),
    PartitionSetProperty.class.getName()
  ).collect(Collectors.toSet());

  /**
   * Static method to apply new EPs to the list of objects.
   *
   * @param objectList the list of objects to apply the EPs to.
   * @param newEP the new EP to apply to the objects.
   * @param dag the DAG to observe.
   */
  public static void applyNewEPs(final List<Object> objectList, final ExecutionProperty<? extends Serializable> newEP,
                                 final IRDAG dag) {
    if (newEP == null || !configurationSpace.contains(newEP.getClass().getName())) { // only handle those in conf space.
      return;
    }

    for (final Object obj: objectList) {
      if (obj instanceof IRVertex) {
        final IRVertex v = (IRVertex) obj;
        final VertexExecutionProperty<?> originalEP = v.getExecutionProperties().stream()
          .filter(ep -> ep.getClass().isAssignableFrom(newEP.getClass())).findFirst().orElse(null);
        v.setPropertyIfPossible((VertexExecutionProperty) newEP);
        if (!dag.checkIntegrity().isPassed()) {
          if (originalEP == null) {
            v.getExecutionProperties().remove((Class<? extends VertexExecutionProperty>) newEP.getClass());
          } else {
            v.setPropertyIfPossible(originalEP);
          }
        }
      } else if (obj instanceof IREdge) {
        final IREdge e = (IREdge) obj;
        final EdgeExecutionProperty<?> originalEP = e.getExecutionProperties().stream()
          .filter(ep -> ep.getClass().isAssignableFrom(newEP.getClass())).findFirst().orElse(null);
        e.setPropertyIfPossible((EdgeExecutionProperty) newEP);
        if (!dag.checkIntegrity().isPassed()) {
          if (originalEP == null) {
            e.getExecutionProperties().remove((Class<? extends EdgeExecutionProperty>) newEP.getClass());
          } else {
            e.setPropertyIfPossible(originalEP);
          }
        }
      }
    }
  }
}
