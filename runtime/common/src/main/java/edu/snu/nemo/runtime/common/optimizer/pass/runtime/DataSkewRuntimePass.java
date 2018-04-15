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
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.eventhandler.RuntimeEventHandler;
import edu.snu.nemo.common.exception.DynamicOptimizationException;

import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalStage;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.nemo.runtime.common.data.HashRange;
import edu.snu.nemo.runtime.common.eventhandler.DynamicOptimizationEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  public PhysicalPlan apply(final PhysicalPlan originalPlan, final Map<String, List<Pair<Integer, Long>>> metricData) {
    // Builder to create new stages.
    final DAGBuilder<PhysicalStage, PhysicalStageEdge> physicalDAGBuilder =
        new DAGBuilder<>(originalPlan.getStageDAG());

    // get edges to optimize
    final List<String> optimizationEdgeIds = metricData.keySet().stream().map(blockId ->
        RuntimeIdGenerator.getRuntimeEdgeIdFromBlockId(blockId)).collect(Collectors.toList());
    final DAG<PhysicalStage, PhysicalStageEdge> stageDAG = originalPlan.getStageDAG();
    final List<PhysicalStageEdge> optimizationEdges = stageDAG.getVertices().stream()
        .flatMap(physicalStage -> stageDAG.getIncomingEdgesOf(physicalStage).stream())
        .filter(physicalStageEdge -> optimizationEdgeIds.contains(physicalStageEdge.getId()))
        .collect(Collectors.toList());

    // Get number of evaluators of the next stage (number of blocks).
    final Integer taskGroupListSize = optimizationEdges.stream().findFirst().orElseThrow(() ->
        new RuntimeException("optimization edges are empty")).getDst().getTaskGroupIds().size();

    // Calculate keyRanges.
    final List<KeyRange> keyRanges = calculateHashRanges(metricData, taskGroupListSize);

    // Overwrite the previously assigned hash value range in the physical DAG with the new range.
    optimizationEdges.forEach(optimizationEdge -> {
      // Update the information.
      final List<KeyRange> taskGroupIdxToHashRange = new ArrayList<>();
      IntStream.range(0, taskGroupListSize).forEach(i -> taskGroupIdxToHashRange.add(keyRanges.get(i)));
      optimizationEdge.setTaskGroupIdxToKeyRange(taskGroupIdxToHashRange);
    });

    return new PhysicalPlan(originalPlan.getId(), physicalDAGBuilder.build(), originalPlan.getTaskIRVertexMap());
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
    return null;
  }
}
