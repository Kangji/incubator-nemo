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
public final class LocationShareAssignmentPass extends AnnotatingPass {

  private static final Logger LOG = LoggerFactory.getLogger(LocationShareAssignmentPass.class);

  private static String bandwidthSpecificationString = "";

  /**
   * Default constructor.
   */
  public LocationShareAssignmentPass() {
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
      if (stageIdToParentStageIds.get(stageId).size() != 1) {
        // The stage is root stage, or has multiple parent stages.
        // Fall back to setting even distribution
        final Map<String, Integer> shares = new HashMap<>();
        final List<String> locations = new ArrayList<>(bandwidthSpecification.getLocations());
        final int defaultShare = parallelism / locations.size();
        final int remainder = parallelism % locations.size();
        for (int i = 0; i < locations.size(); i++) {
          shares.put(locations.get(i), defaultShare + (i < remainder ? 1 : 0));
        }
        stageIdToLocationToNumTaskGroups.put(stageId, shares);
        return;
      }
      final int parentStageId = stageIdToParentStageIds.get(stageId).iterator().next();
      final Map<String, Integer> parentLocationShare = stageIdToLocationToNumTaskGroups.get(parentStageId);
      final Map<String, Double> locationToRatio = new HashMap<>();
      double ratioSum = 0;
      for (final String location : bandwidthSpecification.getLocations()) {
        try {
          final double ratio = getLocationShareToShuffleTimeRatioFor(location, parentLocationShare,
              bandwidthSpecification);
          ratioSum += ratio;
          locationToRatio.put(location, ratio);
        } catch (final ArithmeticException e) {
          // This implies ratio for this location is infinite.
          final Map<String, Integer> shares = new HashMap<>();
          bandwidthSpecification.getLocations().forEach(loc -> shares.put(loc, location.equals(loc) ? parallelism : 0));
          stageIdToLocationToNumTaskGroups.put(stageId, shares);
          return;
        }
      }
      final double coefficient = ((double) parallelism) / ratioSum;
      final Map<String, Integer> shares = new HashMap<>();
      bandwidthSpecification.getLocations().forEach(location -> shares.put(location,
          (int) (locationToRatio.get(location) * coefficient)));
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

  private static double getLocationShareToShuffleTimeRatioFor(final String location,
                                                              final Map<String, Integer> parentLocationShare,
                                                              final BandwidthSpecification bandwidthSpecification) {
    double inverseRatio = 0;
    try {
      for (final String remoteLocation : bandwidthSpecification.getLocations()) {
        if (remoteLocation.equals(location)) {
          continue;
        }
        inverseRatio += ((double) parentLocationShare.get(remoteLocation))
            / ((double) bandwidthSpecification.getBandwidth(remoteLocation, location));
      }
    } catch (final ArithmeticException e) {
      // Bandwidth from a remote location to this location is zero.
      // We're not going to allocate any TaskGroups to this location, since it cannot download intermediate data at all.
      return 0;
    }
    return 1 / inverseRatio;
  }

  /**
   * Bandwidth specification.
   */
  private static final class BandwidthSpecification {
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
          specification.uplinkBandwidth.put(name, up);
          specification.downlinkBandwidth.put(name, down);
        }
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      return specification;
    }

    int getBandwidth(final String source, final String destination) {
      return Math.min(uplinkBandwidth.get(source), downlinkBandwidth.get(destination));
    }

    Set<String> getLocations() {
      return uplinkBandwidth.keySet();
    }
  }
}
