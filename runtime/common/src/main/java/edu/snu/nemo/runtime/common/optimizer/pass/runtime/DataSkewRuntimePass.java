/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.common.optimizer.pass.runtime;

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.eventhandler.RuntimeEventHandler;
import edu.snu.nemo.common.exception.DynamicOptimizationException;

import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.KeyRangeProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.common.data.KeyRange;
import edu.snu.nemo.common.data.HashRange;
import edu.snu.nemo.runtime.common.eventhandler.DynamicOptimizationEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Dynamic optimization pass for handling data skew.
 */
public final class DataSkewRuntimePass implements RuntimePass<Map<String, List<Pair<Integer, Long>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSkewRuntimePass.class.getName());
  private final Set<Class<? extends RuntimeEventHandler>> eventHandlers;

  /**
   * Constructor.
   */
  public DataSkewRuntimePass() {
    this.eventHandlers = Collections.singleton(
        DynamicOptimizationEventHandler.class);
  }

  @Override
  public Set<Class<? extends RuntimeEventHandler>> getEventHandlerClasses() {
    return this.eventHandlers;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dagToOptimize,
                                     final Map<String, List<Pair<Integer, Long>>> metricData) {
    // get edges to optimize
    final List<String> optimizationEdgeIds = metricData.keySet().stream().map(blockId ->
        RuntimeIdGenerator.getRuntimeEdgeIdFromBlockId(blockId)).collect(Collectors.toList());
    final List<IREdge> optimizationEdges = dagToOptimize.getVertices().stream()
        .flatMap(irVertex -> dagToOptimize.getIncomingEdgesOf(irVertex).stream())
        .filter(irEdge -> optimizationEdgeIds.contains(irEdge.getId()))
        .collect(Collectors.toList());

    // Get number of evaluators of the next stage (number of blocks).
    final IREdge targetEdge = optimizationEdges.stream().findFirst()
        .orElseThrow(() -> new RuntimeException("optimization edges are empty"));
    final Integer dstParallelism = targetEdge.getDst().getProperty(ExecutionProperty.Key.Parallelism);

    // Calculate keyRanges.
    final List<KeyRange> keyRanges = calculateHashRanges(metricData, dstParallelism);

    // Overwrite the previously assigned hash value range in the physical DAG with the new range.
    targetEdge.setProperty(KeyRangeProperty.of(keyRanges));
    return dagToOptimize;
  }

  /**
   * Method for calculating key ranges to evenly distribute the skewed metric data.
   *
   * @param metricData        the metric data.
   * @param taskGroupListSize the size of the task group list.
   * @return the list of key ranges calculated.
   */
  @VisibleForTesting
  public List<KeyRange> calculateHashRanges(final Map<String, List<Pair<Integer, Long>>> metricData,
                                            final Integer taskGroupListSize) {
    // NOTE: metricData is made up of a map of blockId to blockSizes.
    // Count the hash range (number of blocks for each block).
    final int maxHashValue = metricData.values().stream()
        .map(list -> list.stream()
            .map(pair -> pair.left())
            .max(Integer::compareTo)
            .orElseThrow(() -> new DynamicOptimizationException("Cannot find max hash value in a block.")))
        .max(Integer::compareTo)
        .orElseThrow(() -> new DynamicOptimizationException("Cannot find max hash value among blocks."));

    // Aggregate metric data.
    final Map<Integer, Long> aggregatedMetricData = new HashMap<>(maxHashValue);
    // for each hash range index, we aggregate the metric data.
    metricData.forEach((blockId, pairs) -> {
      pairs.forEach(pair -> {
        final int key = pair.left();
        if (aggregatedMetricData.containsKey(key)) {
          aggregatedMetricData.compute(key, (existKey, existValue) -> existValue + pair.right());
        } else {
          aggregatedMetricData.put(key, pair.right());
        }
      });
    });

    // Do the optimization using the information derived above.
    final Long totalSize = aggregatedMetricData.values().stream().mapToLong(n -> n).sum(); // get total size
    final Long idealSizePerTaskGroup = totalSize / taskGroupListSize; // and derive the ideal size per task group
    LOG.info("idealSizePerTaskgroup {} = {}(totalSize) / {}(taskGroupListSize)",
        idealSizePerTaskGroup, totalSize, taskGroupListSize);

    // find HashRanges to apply (for each blocks of each block).
    final List<KeyRange> keyRanges = new ArrayList<>(taskGroupListSize);
    int startingHashValue = 0;
    int finishingHashValue = 1; // initial values
    Long currentAccumulatedSize = aggregatedMetricData.getOrDefault(startingHashValue, 0L);
    for (int i = 1; i <= taskGroupListSize; i++) {
      if (i != taskGroupListSize) {
        final Long idealAccumulatedSize = idealSizePerTaskGroup * i; // where we should end
        // find the point while adding up one by one.
        while (currentAccumulatedSize < idealAccumulatedSize) {
          currentAccumulatedSize += aggregatedMetricData.getOrDefault(finishingHashValue, 0L);
          finishingHashValue++;
        }

        final Long oneStepBack =
            currentAccumulatedSize - aggregatedMetricData.getOrDefault(finishingHashValue - 1, 0L);
        final Long diffFromIdeal = currentAccumulatedSize - idealAccumulatedSize;
        final Long diffFromIdealOneStepBack = idealAccumulatedSize - oneStepBack;
        // Go one step back if we came too far.
        if (diffFromIdeal > diffFromIdealOneStepBack) {
          finishingHashValue--;
          currentAccumulatedSize -= aggregatedMetricData.getOrDefault(finishingHashValue, 0L);
        }

        // assign appropriately
        keyRanges.add(i - 1, HashRange.of(startingHashValue, finishingHashValue));
        startingHashValue = finishingHashValue;
      } else { // last one: we put the range of the rest.
        keyRanges.add(i - 1, HashRange.of(startingHashValue, maxHashValue + 1));
      }
    }
    return keyRanges;
  }
}
