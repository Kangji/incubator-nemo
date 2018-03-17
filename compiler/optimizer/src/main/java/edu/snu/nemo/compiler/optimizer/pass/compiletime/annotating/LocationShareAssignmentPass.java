/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.LocationSharesProperty;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Computes and assigns appropriate share of locations to each stage,
 * with respect to bandwidth restrictions of locations.
 *
 * <h3>Assumptions</h3>
 * This pass assumes no skew in input or intermediate data, so that the number of TaskGroups assigned to a location
 * is proportional to the data size handled by the location.
 * Also, this pass assumes stages with empty map as {@link LocationSharesProperty} are assigned to locations evenly.
 * For example, if source splits are not distributed evenly, any source location-aware scheduling policy will
 * assign TaskGroups unevenly.
 */
public final class LocationShareAssignmentPass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public LocationShareAssignmentPass() {
    super(ExecutionProperty.Key.LocationShares,
        Stream.of(ExecutionProperty.Key.StageId, ExecutionProperty.Key.Parallelism).collect(Collectors.toSet()));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final Map<Integer, Map<String, Integer>> stageIdToLocationShares = getStageIdToLocationToNumTaskGroups(dag,
        getStageIdToParentStageIds(dag));
    dag.topologicalDo(irVertex -> irVertex.setProperty(LocationSharesProperty.of(stageIdToLocationShares
        .get(irVertex.getProperty(ExecutionProperty.Key.StageId)))));
    return dag;
  }

  private static Map<Integer, Set<Integer>> getStageIdToParentStageIds(final DAG<IRVertex, IREdge> dag) {
    final Map<Integer, Set<Integer>> stageIdToParentStageIds = new HashMap<>();
    dag.getVertices().forEach(irVertex -> {
      final int stageId = irVertex.getProperty(ExecutionProperty.Key.StageId);
      final Set<Integer> parentStageIds = stageIdToParentStageIds.computeIfAbsent(stageId, id -> new HashSet<>());
      dag.getIncomingEdgesOf(irVertex).forEach(irEdge ->
          parentStageIds.add(irEdge.getSrc().getExecutionProperties().get(ExecutionProperty.Key.StageId)));
    });
    return stageIdToParentStageIds;
  }

  private static Map<Integer, Map<String, Integer>> getStageIdToLocationToNumTaskGroups(
      final DAG<IRVertex, IREdge> dag, final Map<Integer, Set<Integer>> stageIdToParentStageIds) {
    final Map<Integer, Map<String, Integer>> stageIdToLocationToNumTaskGroups = new HashMap<>();
    dag.topologicalDo(irVertex -> {
      final int stageId = irVertex.getProperty(ExecutionProperty.Key.StageId);
      if (stageIdToLocationToNumTaskGroups.containsKey(stageId)) {
        // We have already computed LocationSharesProperty for this stage
        return;
      }
      if (stageIdToParentStageIds.get(stageId).size() != 1) {
        // The stage is root stage, or has multiple parent stages.
        // Fall back to setting no requirements for locations to execute this stage
        stageIdToLocationToNumTaskGroups.put(stageId, Collections.emptyMap());
      } else {
        // to-do.
        stageIdToLocationToNumTaskGroups.put(stageId, Collections.emptyMap());
      }
    });
    return stageIdToLocationToNumTaskGroups;
  }
}
