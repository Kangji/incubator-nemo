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

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.LocationSharesProperty;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.*;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Computes and assigns appropriate share of locations to each stage,
 * with respect to bandwidth restrictions of locations. If bandwidth information is not given, this pass does nothing.
 *
 * <h3>Assumptions</h3>
 * This pass assumes no skew in input or intermediate data, so that the number of TaskGroups assigned to a location
 * is proportional to the data size handled by the location.
 * Also, this pass assumes stages with empty map as {@link LocationSharesProperty} are assigned to locations evenly.
 * For example, if source splits are not distributed evenly, any source location-aware scheduling policy will
 * assign TaskGroups unevenly.
 * Also, this pass assumes network bandwidth to be the bottleneck. Each location should have enough capacity to run
 * TaskGroups immediately as scheduler attempts to schedule a TaskGroup.
 */
public final class ShuffleLocationAssignmentPass extends AnnotatingPass {

  private static final int OBJECTIVE_COEFFICIENT_INDEX = 0;
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleLocationAssignmentPass.class);

  private static String bandwidthSpecificationString = "";

  /**
   * Default constructor.
   */
  public ShuffleLocationAssignmentPass() {
    super(ExecutionProperty.Key.LocationShares,
        Stream.of(ExecutionProperty.Key.StageId, ExecutionProperty.Key.Parallelism).collect(Collectors.toSet()));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    if (bandwidthSpecificationString.isEmpty()) {
      dag.topologicalDo(irVertex -> irVertex.setProperty(LocationSharesProperty.of(Collections.emptyMap())));
    } else {
      final Map<Integer, Map<String, Integer>> stageIdToLocationShares = getStageIdToLocationToNumTaskGroups(dag,
          getStageIdToParentStageIds(dag), BandwidthSpecification.fromJsonString(bandwidthSpecificationString));
      dag.topologicalDo(irVertex -> irVertex.setProperty(LocationSharesProperty.of(stageIdToLocationShares
          .get(irVertex.getProperty(ExecutionProperty.Key.StageId)))));
    }
    return dag;
  }

  public static void setBandwidthSpecificationString(final String value) {
    bandwidthSpecificationString = value;
  }

  private static Map<Integer, Set<Integer>> getStageIdToParentStageIds(final DAG<IRVertex, IREdge> dag) {
    final Map<Integer, Set<Integer>> stageIdToParentStageIds = new HashMap<>();
    dag.getVertices().forEach(irVertex -> {
      final int stageId = irVertex.getProperty(ExecutionProperty.Key.StageId);
      final Set<Integer> parentStageIds = stageIdToParentStageIds.computeIfAbsent(stageId, id -> new HashSet<>());
      dag.getIncomingEdgesOf(irVertex).stream()
          .map(irEdge -> (int) irEdge.getSrc().getExecutionProperties().get(ExecutionProperty.Key.StageId))
          .filter(id -> id != stageId)
          .forEach(parentStageIds::add);
    });
    return stageIdToParentStageIds;
  }

  private static Map<Integer, Map<String, Integer>> getStageIdToLocationToNumTaskGroups(
      final DAG<IRVertex, IREdge> dag,
      final Map<Integer, Set<Integer>> stageIdToParentStageIds,
      final BandwidthSpecification bandwidthSpecification) {
    final Map<Integer, Map<String, Integer>> stageIdToLocationToNumTaskGroups = new HashMap<>();
    dag.topologicalDo(irVertex -> {
      final int stageId = irVertex.getProperty(ExecutionProperty.Key.StageId);
      final int parallelism = irVertex.getProperty(ExecutionProperty.Key.Parallelism);
      if (stageIdToLocationToNumTaskGroups.containsKey(stageId)) {
        // We have already computed LocationSharesProperty for this stage
        return;
      }
      if (stageIdToParentStageIds.get(stageId).size() == 0) {
        // The stage is root stage.
        // Fall back to setting even distribution
        final Map<String, Integer> shares = new HashMap<>();
        final List<String> locations = bandwidthSpecification.getLocations();
        final int defaultShare = parallelism / locations.size();
        final int remainder = parallelism % locations.size();
        for (int i = 0; i < locations.size(); i++) {
          shares.put(locations.get(i), defaultShare + (i < remainder ? 1 : 0));
        }
        stageIdToLocationToNumTaskGroups.put(stageId, shares);
        return;
      }
      final Map<String, Integer> parentLocationShares = new HashMap<>();
      for (final int parentStageId : stageIdToParentStageIds.get(stageId)) {
        final Map<String, Integer> shares = stageIdToLocationToNumTaskGroups.get(parentStageId);
        for (final Map.Entry<String, Integer> element : shares.entrySet()) {
          parentLocationShares.putIfAbsent(element.getKey(), 0);
          parentLocationShares.put(element.getKey(),
              element.getValue() + parentLocationShares.get(element.getKey()));
        }
      }
      final double[] ratios = optimize(bandwidthSpecification, parentLocationShares);
      final Map<String, Integer> shares = new HashMap<>();
      for (int i = 0; i < bandwidthSpecification.getLocations().size(); i++) {
        shares.put(bandwidthSpecification.getLocations().get(i), (int) (ratios[i] * parallelism));
      }
      int remainder = parallelism - shares.values().stream().mapToInt(i -> i).sum();
      for (final String location : shares.keySet()) {
        if (remainder == 0) {
          break;
        }
        shares.put(location, shares.get(location) + 1);
        remainder--;
      }
      stageIdToLocationToNumTaskGroups.put(stageId, shares);
    });
    return stageIdToLocationToNumTaskGroups;
  }

  private static double[] optimize(final BandwidthSpecification bandwidthSpecification,
                                   final Map<String, Integer> parentLocationShares) {
    final int parentParallelism = parentLocationShares.values().stream().mapToInt(i -> i).sum();
    final List<String> locations = bandwidthSpecification.getLocations();
    final List<LinearConstraint> constraints = new ArrayList<>();
    final int coefficientVectorSize = locations.size() + 1;

    for (int i = 0; i < locations.size(); i++) {
      final String location = locations.get(i);
      final int locationCoefficientIndex = i + 1;
      final int parentParallelismOnThisLocation = parentLocationShares.get(location);

      // Upload bandwidth
      final double[] uploadCoefficientVector = new double[coefficientVectorSize];
      uploadCoefficientVector[OBJECTIVE_COEFFICIENT_INDEX] = bandwidthSpecification.up(location);
      uploadCoefficientVector[locationCoefficientIndex] = parentParallelismOnThisLocation;
      constraints.add(new LinearConstraint(uploadCoefficientVector, Relationship.GEQ,
          parentParallelismOnThisLocation));

      // Download bandwidth
      final double[] downloadCoefficientVector = new double[coefficientVectorSize];
      downloadCoefficientVector[OBJECTIVE_COEFFICIENT_INDEX] = bandwidthSpecification.down(location);
      downloadCoefficientVector[locationCoefficientIndex] = parentParallelismOnThisLocation - parentParallelism;
      constraints.add(new LinearConstraint(downloadCoefficientVector, Relationship.GEQ, 0));

      // The coefficient is non-negative
      final double[] nonNegativeCoefficientVector = new double[coefficientVectorSize];
      nonNegativeCoefficientVector[locationCoefficientIndex] = 1;
      constraints.add(new LinearConstraint(nonNegativeCoefficientVector, Relationship.GEQ, 0));
    }

    // The sum of all coefficient is 1
    final double[] sumCoefficientVector = new double[coefficientVectorSize];
    for (int i = 0; i < locations.size(); i++) {
      sumCoefficientVector[OBJECTIVE_COEFFICIENT_INDEX + 1 + i] = 1;
    }
    constraints.add(new LinearConstraint(sumCoefficientVector, Relationship.EQ, 1));

    // Objective
    final double[] objectiveCoefficientVector = new double[coefficientVectorSize];
    objectiveCoefficientVector[OBJECTIVE_COEFFICIENT_INDEX] = 1;
    final LinearObjectiveFunction objectiveFunction = new LinearObjectiveFunction(objectiveCoefficientVector, 0);

    // Solve
    final PointValuePair solved = new SimplexSolver().optimize(
        new LinearConstraintSet(constraints), objectiveFunction, GoalType.MINIMIZE);

    return Arrays.copyOfRange(solved.getPoint(), OBJECTIVE_COEFFICIENT_INDEX + 1, coefficientVectorSize);
  }

  /**
   * Bandwidth specification.
   */
  private static final class BandwidthSpecification {
    private final List<String> locations = new ArrayList<>();
    private final Map<String, Integer> uplinkBandwidth = new HashMap<>();
    private final Map<String, Integer> downlinkBandwidth = new HashMap<>();

    private BandwidthSpecification() {
    }

    static BandwidthSpecification fromJsonString(final String jsonString) {
      final BandwidthSpecification specification = new BandwidthSpecification();
      try {
        final ObjectMapper objectMapper = new ObjectMapper();
        final TreeNode jsonRootNode = objectMapper.readTree(jsonString);
        for (int i = 0; i < jsonRootNode.size(); i++) {
          final TreeNode locationNode = jsonRootNode.get(i);
          final String name = locationNode.get("name").traverse().nextTextValue();
          final int up = locationNode.get("up").traverse().getIntValue();
          final int down = locationNode.get("down").traverse().getIntValue();
          specification.locations.add(name);
          specification.uplinkBandwidth.put(name, up);
          specification.downlinkBandwidth.put(name, down);
        }
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      return specification;
    }

    int up(final String location) {
      return uplinkBandwidth.get(location);
    }

    int down(final String location) {
      return downlinkBandwidth.get(location);
    }

    List<String> getLocations() {
      return locations;
    }
  }
}
