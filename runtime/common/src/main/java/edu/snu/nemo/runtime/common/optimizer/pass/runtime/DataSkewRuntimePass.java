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
public final class DataSkewRuntimePass implements RuntimePass<Pair<List<String>, Map<Integer, Long>>> {
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
                                     final Pair<List<String>, Map<Integer, Long>> metricData) {
    // get edges to optimize
    final List<String> optimizationEdgeIds = metricData.left().stream().map(blockId ->
        RuntimeIdGenerator.getRuntimeEdgeIdFromBlockId(blockId))
        .map(RuntimeIdGenerator::getIrEdgeIdFromRuntimeEdgeId)
        .collect(Collectors.toList());
    final List<IREdge> optimizationEdges = dagToOptimize.getVertices().stream()
        .flatMap(irVertex -> dagToOptimize.getIncomingEdgesOf(irVertex).stream())
        .filter(irEdge -> optimizationEdgeIds.contains(irEdge.getId()))
        .collect(Collectors.toList());

    // Get number of evaluators of the next stage (number of blocks).
    final IREdge targetEdge = optimizationEdges.stream().findFirst()
        .orElseThrow(() -> new RuntimeException("optimization edges are empty"));
    final Integer dstParallelism = targetEdge.getDst().getProperty(ExecutionProperty.Key.Parallelism);

    // Calculate keyRanges.
    final Map<Integer, Pair<KeyRange, Boolean>> keyRanges = calculateHashRanges(metricData.right(), dstParallelism);

    // Overwrite the previously assigned hash value range in the physical DAG with the new range.
    targetEdge.setProperty(KeyRangeProperty.of(keyRanges));
    return dagToOptimize;
  }

  /**
   * Method for calculating key ranges to evenly distribute the skewed metric data.
   *
   * @param aggregatedMetricDataMap        the metric data.
   * @param taskGroupListSize the size of the task group list.
   * @return the list of key ranges calculated.
   */
  @VisibleForTesting
  public Map<Integer, Pair<KeyRange, Boolean>> calculateHashRanges(final Map<Integer, Long> aggregatedMetricDataMap,
                                            final Integer taskGroupListSize) {
    // NOTE: aggregatedMetricDataMap is made up of a map of (hash value, blockSize).
    // Get the max hash value.
    final int maxHashValue = aggregatedMetricDataMap.keySet().stream()
            .max(Integer::compareTo)
            .orElseThrow(() -> new DynamicOptimizationException("Cannot find max hash value among blocks."));

    List<Map.Entry<Integer, Long>> sortedMetricDataMap = aggregatedMetricDataMap.entrySet().stream()
        .sorted((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()))
        .collect(Collectors.toList());

    List<Integer> hotHashes = new ArrayList<>();
    for (int i = 0; i < sortedMetricDataMap.size(); i++) {
      hotHashes.add(sortedMetricDataMap.get(i).getKey());
      LOG.info("HotHash: Hash {} Size {}", sortedMetricDataMap.get(i).getKey(),
          sortedMetricDataMap.get(i).getValue());
    }

    // Do the optimization using the information derived above.
    final Long totalSize = aggregatedMetricDataMap.values().stream().mapToLong(n -> n).sum(); // get total size
    final Long idealSizePerTaskGroup = totalSize / taskGroupListSize; // and derive the ideal size per task group
    LOG.info("idealSizePerTaskgroup {} = {}(totalSize) / {}(taskGroupListSize)",
        idealSizePerTaskGroup, totalSize, taskGroupListSize);

    // find HashRanges to apply (for each blocks of each block).
    final Map<Integer, Pair<KeyRange, Boolean>> keyRanges = new HashMap<>(taskGroupListSize);
    int startingHashValue = 0;
    int finishingHashValue = 1; // initial values
    Long currentAccumulatedSize = aggregatedMetricDataMap.getOrDefault(startingHashValue, 0L);
    Long prevAccumulatedSize = 0L;
    for (int i = 1; i <= taskGroupListSize; i++) {
      if (i != taskGroupListSize) {
        final Long idealAccumulatedSize = idealSizePerTaskGroup * i; // where we should end
        // find the point while adding up one by one.
        while (currentAccumulatedSize < idealAccumulatedSize) {
          currentAccumulatedSize += aggregatedMetricDataMap.getOrDefault(finishingHashValue, 0L);
          finishingHashValue++;
        }

        final Long oneStepBack =
            currentAccumulatedSize - aggregatedMetricDataMap.getOrDefault(finishingHashValue - 1, 0L);
        final Long diffFromIdeal = currentAccumulatedSize - idealAccumulatedSize;
        final Long diffFromIdealOneStepBack = idealAccumulatedSize - oneStepBack;
        // Go one step back if we came too far.
        if (diffFromIdeal > diffFromIdealOneStepBack) {
          finishingHashValue--;
          currentAccumulatedSize -= aggregatedMetricDataMap.getOrDefault(finishingHashValue, 0L);
        }

        boolean isHotHash = false;
        for (int h = startingHashValue; h < finishingHashValue; h++) {
          if (hotHashes.contains(h)) {
            isHotHash = true;
            break;
          }
        }

        keyRanges.put(i - 1, Pair.of(HashRange.of(startingHashValue, finishingHashValue), isHotHash));
        // assign appropriately
        LOG.info("KeyRange {}~{}, Size {}, isHotHash {}", startingHashValue, finishingHashValue,
            currentAccumulatedSize - prevAccumulatedSize, isHotHash);
        prevAccumulatedSize = currentAccumulatedSize;
        startingHashValue = finishingHashValue;
      } else { // last one: we put the range of the rest.
        boolean isHotHash = false;
        for (int h = startingHashValue; h < maxHashValue + 1; h++) {
          if (hotHashes.contains(h)) {
            isHotHash = true;
            break;
          }
        }

        keyRanges.put(i - 1, Pair.of(HashRange.of(startingHashValue, maxHashValue + 1), isHotHash));
        LOG.info("KeyRange {}~{}, Size {}", startingHashValue, maxHashValue + 1,
            currentAccumulatedSize - prevAccumulatedSize);
      }
    }
    return keyRanges;
  }
}
