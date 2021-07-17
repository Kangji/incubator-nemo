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
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.exception.SchedulingException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ShuffleExecutorSetProperty;
import org.apache.nemo.compiler.frontend.beam.transform.CombineTransform;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Pass for inserting intermediate aggregator for partial shuffle.
 */
@Requires(ParallelismProperty.class)
public class IntermediateAccumulatorInsertionPass extends ReshapingPass {
  /**
   * Default constructor.
   */
  public IntermediateAccumulatorInsertionPass() {
    super(IntermediateAccumulatorInsertionPass.class);
  }

  /**
   * Insert accumulator vertex based on network hierarchy.
   *
   * @param irdag irdag to apply pass.
   * @return modified irdag.
   */
  @Override
  public IRDAG apply(final IRDAG irdag) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      final Map<String, ArrayList<String>> map = mapper.readValue(
        new File(Util.fetchProjectRootPath() + "/bin/labeldict.json"), Map.class);

      irdag.topologicalDo(v -> {
        // For now, only handle beam combine transform with intermediate combine fn.
        final List<IREdge> incomingShuffleEdges = irdag.getIncomingEdgesOf(v).stream()
          .filter(e -> CommunicationPatternProperty.Value.SHUFFLE
            .equals(e.getPropertyValue(CommunicationPatternProperty.class)
              .orElse(CommunicationPatternProperty.Value.ONE_TO_ONE)))
          .collect(Collectors.toList());
        if (v instanceof OperatorVertex && ((OperatorVertex) v).getTransform() instanceof CombineTransform
          && incomingShuffleEdges.size() == 1) { // redundant, but just in case
          final CombineTransform finalCombineStreamTransform = (CombineTransform) ((OperatorVertex) v).getTransform();
          if (finalCombineStreamTransform.getIntermediateCombine().isPresent()) {
            handleDataTransferFor(irdag, map, finalCombineStreamTransform, incomingShuffleEdges, v, 10F);
          }
        }
      });
      return irdag;
    } catch (final Exception e) {
      throw new SchedulingException(e);
    }
  }

  private static void handleDataTransferFor(final IRDAG irdag,
                                            final Map<String, ArrayList<String>> map,
                                            final CombineTransform finalCombineStreamTransform,
                                            final List<IREdge> incomingShuffleEdges,
                                            final IRVertex dst,
                                            final Float threshold) {
    int previousParallelism = incomingShuffleEdges.stream()
      .mapToInt(e -> e.getSrc().getPropertyValue(ParallelismProperty.class).get()).sum();
    final int minParallelism = dst.getPropertyValue(ParallelismProperty.class).get();

    final int mapSize = map.size();
    final int indexToCheckFrom = mapSize - previousParallelism;
    Float previousDistance = 0F;

    for (int i = indexToCheckFrom; i < mapSize; i++) {
      final float currentDistance = Float.parseFloat(map.get(String.valueOf(i)).get(1));
      if (previousDistance != 0 && currentDistance > threshold * previousDistance
        && previousParallelism * 2 / 3 >= mapSize - i + 1 && mapSize - i + 1 > minParallelism) {
        final Integer targetNumberOfSets = mapSize - i;
        final HashSet<HashSet<String>> setsOfExecutors = getTargetNumberOfExecutorSetsFrom(map, targetNumberOfSets);

        final CombineTransform<?, ?, ?> intermediateCombineStreamTransform =
          (CombineTransform) finalCombineStreamTransform.getIntermediateCombine().get();
        final OperatorVertex accumulatorVertex = new OperatorVertex(intermediateCombineStreamTransform);

        dst.copyExecutionPropertiesTo(accumulatorVertex);
        accumulatorVertex.setProperty(ParallelismProperty.of(setsOfExecutors.size()));
        accumulatorVertex.setProperty(ShuffleExecutorSetProperty.of(setsOfExecutors));

        irdag.insert(accumulatorVertex, incomingShuffleEdges);

        previousParallelism = setsOfExecutors.size();
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
