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

package org.apache.nemo.compiler.optimizer.pass.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.exception.SchedulingException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.MessageIdEdgeProperty;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.apache.nemo.compiler.frontend.beam.transform.CombineTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A pass that inserts an intermediate accumulator in between combiners and reducers.
 * This pass is currently specific for BEAM applications, and has no effect on other applications.
 * This pass can be extended to other applications for it to take better effect.
 */
public final class IntermediateAccumulatorInsertionPass extends RunTimePass<Map<String, HashSet<String>>> {
  private static final Logger LOG = LoggerFactory.getLogger(IntermediateAccumulatorInsertionPass.class.getName());
  public static final String EXECUTOR_SOURCE_KEY = "source_executors_set";

  /**
   * Default constructor.
   */
  public IntermediateAccumulatorInsertionPass() {
  }

  @Override
  public IRDAG apply(final IRDAG irdag, final Message<Map<String, HashSet<String>>> dataSourceExecutors) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      final Map<String, ArrayList<String>> map = mapper.readValue(
        new File(Util.fetchProjectRootPath() + "/bin/labeldict.json"), Map.class);

      final Integer messageId = dataSourceExecutors.getMessageId();
      irdag.topologicalDo(v -> {
        if (v.getPropertyValue(MessageIdVertexProperty.class).isPresent()
          && v.getPropertyValue(MessageIdVertexProperty.class).get().equals(messageId)) {
          // Insert an intermediate GBKTransform before the final combining vertex
          final List<IREdge> incomingShuffleEdges = irdag.getIncomingEdgesOf(v).stream()
            .filter(e -> CommunicationPatternProperty.Value.SHUFFLE
              .equals(e.getPropertyValue(CommunicationPatternProperty.class)
                .orElse(CommunicationPatternProperty.Value.ONE_TO_ONE)))
            .collect(Collectors.toList());

          final CombineTransform<?, ?, ?> finalCombineStreamTransform =
            (CombineTransform<?, ?, ?>) ((OperatorVertex) v).getTransform();

          // Remove the message ID property from the destination of the incoming shuffle edges and the shuffle edges,
          // as all of the insertion is complete, and we don't want to trigger additional insertions afterwards.
          incomingShuffleEdges.forEach(e -> {
            e.getDst().getExecutionProperties().remove(MessageIdVertexProperty.class);
            e.getDst().getExecutionProperties().remove(BarrierProperty.class);
            final HashSet<Integer> edgeMessageIDs = e.getPropertyValue(MessageIdEdgeProperty.class).get();
            LOG.error("EDGE MESSAGE ID: {}", edgeMessageIDs);
            edgeMessageIDs.remove(messageId);
            if (edgeMessageIDs.isEmpty()) {
              e.getExecutionProperties().remove(MessageIdEdgeProperty.class);
            }
          });

          // Insert vertices that accumulate data hierarchically.
          handleDataTransferFor(irdag, map, dataSourceExecutors.getMessageValue().get(EXECUTOR_SOURCE_KEY),
            finalCombineStreamTransform, incomingShuffleEdges, 10F);
        } // else if (v instanceof OperatorVertex && ((OperatorVertex) v).getTransform() instanceof GBKTransform) {
        // }
      });
      return irdag;
    } catch (final Exception e) {
      throw new SchedulingException(e);
    }
  }

  private static void handleDataTransferFor(final IRDAG irdag,
                                            final Map<String, ArrayList<String>> map,
                                            final HashSet<String> dataSourceExecutors,
                                            final CombineTransform<?, ?, ?> finalCombineStreamTransform,
                                            final List<IREdge> incomingShuffleEdges,
                                            final Float threshold) {

    // Note that if there is no previous number of sets, we use the number of data source executors.
    List<IREdge> updatedIncomingShuffleEdges = incomingShuffleEdges;
    int previousNumOfSets = incomingShuffleEdges.stream()
      .mapToInt(e -> e.getSrc().getPropertyValue(ShuffleExecutorSetProperty.class)
        .orElse(new HashSet<>()).size())
      .max().orElse(0);

    // Max value is 2/3 * previousNumOfSets, min value is 1.
    // We traverse from the max to the min value, and compare the distance, and find the value where the distance
    // becomes greater than the previous distance * threshold.
    final int max = previousNumOfSets == 0 ? dataSourceExecutors.size() : previousNumOfSets * 2 / 3;
    final int mapSize = map.size();  // indicates the number of executors * 2 - 1.
    final int indexToCheckFrom = mapSize - max;
    Float previousDistance = 0F;
    if (previousNumOfSets == 0) {
      previousNumOfSets = dataSourceExecutors.size();
    }

    for (int i = indexToCheckFrom; i < mapSize; i++) {
      final float currentDistance = Float.parseFloat(map.get(String.valueOf(i)).get(1));
      if (previousDistance != 0 && currentDistance > threshold * previousDistance
        && previousNumOfSets * 2 / 3 >= mapSize - i) {
        final CombineTransform<?, ?, ?> intermediateCombineStreamTransform =
          finalCombineStreamTransform.getIntermediateCombine().get();
        final OperatorVertex intermediateAccumulatorVertex =
          new OperatorVertex(intermediateCombineStreamTransform);
        irdag.insert(intermediateAccumulatorVertex, updatedIncomingShuffleEdges);
        updatedIncomingShuffleEdges = irdag.getOutgoingEdgesOf(intermediateAccumulatorVertex).stream()
          .filter(e -> CommunicationPatternProperty.Value.SHUFFLE
            .equals(e.getPropertyValue(CommunicationPatternProperty.class)
              .orElse(CommunicationPatternProperty.Value.ONE_TO_ONE)))
          .collect(Collectors.toList());

        // Calculate the number of sets and set the property.
        final Integer targetNumberOfSets = mapSize - i;
        final HashSet<HashSet<String>> setsOfExecutors = getTargetNumberOfExecutorSetsFrom(map, targetNumberOfSets);
        intermediateAccumulatorVertex.setProperty(ShuffleExecutorSetProperty.of(setsOfExecutors));
        previousNumOfSets = setsOfExecutors.size();

        // Calculate the parallelism and set the property
        final Supplier<Stream<Pair<Integer, String>>> taskIndexToExecutorID = () ->
          incomingShuffleEdges.stream()
            .flatMap(e -> e.getSrc().getPropertyValue(TaskIndexToExecutorIDProperty.class).get().entrySet().stream())
            .map(e -> Pair.of(e.getKey(), e.getValue().get(e.getValue().size() - 1)));

        final int parallelism = Long.valueOf(setsOfExecutors.stream()
          .mapToLong(hs -> taskIndexToExecutorID.get().filter(p -> hs.contains(p.right())).count())
          .map(l -> l >= 3 ? l * 2 / 3 : l)
          .sum()).intValue();

        intermediateAccumulatorVertex.setProperty(ParallelismProperty.of(parallelism));
      }
      previousDistance = currentDistance;
    }
  }

  private static HashSet<HashSet<String>> getTargetNumberOfExecutorSetsFrom(final Map<String, ArrayList<String>> map,
                                                                            final Integer targetNumber) {
    final HashSet<HashSet<String>> result = new HashSet<>();
    final Integer index = map.size() - targetNumber;
    final List<String> indicesToCheck = IntStream.range(0, index)
      .map(i -> -i).sorted().map(i -> -i)
      .mapToObj(String::valueOf)
      .collect(Collectors.toList());
    Arrays.asList(map.get(String.valueOf(index)).get(0).split("\\+"))
      .forEach(key -> result.add(recursivelyExtractExecutorsFrom(map, key, indicesToCheck)));

    while (!indicesToCheck.isEmpty()) {
      result.add(recursivelyExtractExecutorsFrom(map, indicesToCheck.get(0), indicesToCheck));
    }

    return result;
  }

  private static HashSet<String> recursivelyExtractExecutorsFrom(final Map<String, ArrayList<String>> map,
                                                                 final String key,
                                                                 final List<String> indicesToCheck) {
    indicesToCheck.remove(key);
    final HashSet<String> result = new HashSet<>();
    final List<String> indices = Arrays.asList(map.get(key).get(0).split("\\+"));
    if (indices.size() == 1) {
      result.add(indices.get(0));
    } else {
      indices.forEach(index -> result.addAll(recursivelyExtractExecutorsFrom(map, index, indicesToCheck)));
    }
    return result;
  }
}
