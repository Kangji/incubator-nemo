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

package org.apache.nemo.runtime.master.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.exception.SchedulingException;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.PipeManagerMaster;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This policy chooses a set of Executors, considering the locality and the minimum running tasks.
 */
@ThreadSafe
@DriverSide
public final class LocalityOccupancySchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(LocalityOccupancySchedulingPolicy.class.getName());

  private final PipeManagerMaster pipeManagerMaster;
  private final ExecutorRegistry registry;

  @Inject
  private LocalityOccupancySchedulingPolicy(final PipeManagerMaster pipeManagerMaster,
                                            final ExecutorRegistry registry) {
    this.pipeManagerMaster = pipeManagerMaster;
    this.registry = registry;
  }

  @Override
  public ExecutorRepresenter selectExecutor(final Collection<ExecutorRepresenter> executors, final Task task) {
    if (executors.isEmpty()) {
      throw new SchedulingException("No executors available for scheduling");
    }

    if (pipeManagerMaster.isActive()) {
      final OptionalDouble avgOccupancy =
        executors.stream()
          .mapToInt(ExecutorRepresenter::getNumOfRunningTasks)
          .average();

      LOG.error("Using PipeManagerMaster!!");
      final List<String> dataLocationExecutorNodeNames = task.getTaskIncomingEdges().stream()
        .map(e -> pipeManagerMaster.getLocation(e.getId(), RuntimeIdManager.getIndexFromTaskId(task.getTaskId())))
        .map(registry::executorIdToNodeName)
        .collect(Collectors.toList());

      return candidateDataLocation(dataLocationExecutorNodeNames, executors, avgOccupancy
        .orElseThrow(() -> new SchedulingException("Cannot find average occupancy of the given executors")));

    } else {
      final OptionalInt minOccupancy =
        executors.stream()
          .mapToInt(ExecutorRepresenter::getNumOfRunningTasks)
          .min();

      return executors.stream()
        .filter(executor -> executor.getNumOfRunningTasks() == minOccupancy.getAsInt())
        .findFirst()
        .orElseThrow(() -> new SchedulingException("No executor found with the minimum occupancy"));
    }
  }

  /**
   * Extract the candidate executors from the source data locations of the intermediate data.
   * @param dataSourceExecutors source executors.
   * @return the executor representer of the candidate node.
   */
  private static ExecutorRepresenter candidateDataLocation(final List<String> dataSourceExecutors,
                                                           final Collection<ExecutorRepresenter> executors,
                                                           final Double avgOccupancy) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      final Map<String, ArrayList<String>> map = mapper.readValue(
        new File(Util.fetchProjectRootPath() + "/bin/labeldict.json"), Map.class);

      // Pair.left is the list of nodes, Pair.right is the ratio of the distance, from the last distance.
      // Ex. if the ratio is 2, that group of nodes have 2x distance compared to the last group of nodes.
      final List<List<String>> list = new ArrayList<>();
      map.forEach((k, v) ->
        list.add(groupsToListOfNodes(map, v.get(0).split("\\+"))));

      for (final List<String> candidates: list) {
        LOG.error("checking for {} in {}", dataSourceExecutors, candidates);
        if (candidates.containsAll(dataSourceExecutors)) {
          final Integer idx = list.indexOf(candidates);
          return handleDataTransferFor(map, idx.toString(), executors, 10F);
        }
      }

      // Just return the one with the min occupancy.
      return executors.stream()
        .min(Comparator.comparingInt(ExecutorRepresenter::getNumOfRunningTasks))
        .orElseThrow(() -> new RuntimeException("Executor doesn't exist"));
    } catch (final Exception e) {
      throw new SchedulingException(e);
    }
  }

  private static ExecutorRepresenter handleDataTransferFor(final Map<String, ArrayList<String>> map,
                                                           final String key,
                                                           final Collection<ExecutorRepresenter> executors,
                                                           final Float threshold) {
    final ArrayList<String> group = map.get(key);
    if (group.get(1).equals("0")) {
      return executors.stream()
        .filter(executor -> executor.getNodeName().equals(group.get(0)))
        .min(Comparator.comparingInt(ExecutorRepresenter::getNumOfRunningTasks))
        .orElseThrow(() -> new RuntimeException("No such executor"));
    }

    final ArrayList<ExecutorRepresenter> candidates = new ArrayList<>();
    for (final String idx: group.get(0).split("\\+")) {
      final ArrayList<String> childGroup = map.get(idx);
      if (childGroup.get(0).matches("\\d+")
        && Float.parseFloat(group.get(1)) > Float.parseFloat(childGroup.get(1)) * threshold) {
        candidates.add(handleDataTransferFor(map, childGroup.get(0), executors, threshold));
      } else {
        candidates.addAll(executors.stream()
          .filter(executor -> groupsToListOfNodes(map, childGroup.get(0).split("\\+")).contains(executor.getNodeName()))
          .collect(Collectors.toList()));
      }
    }
    final ExecutorRepresenter candidate = candidates.stream()
      .min(Comparator.comparingInt(ExecutorRepresenter::getNumOfRunningTasks))
      .orElseThrow(() -> new RuntimeException("Executors were not chosen"));
    // TODO: handle the data transfer to candidate
    return candidate;
  }

  private static List<String> groupsToListOfNodes(final Map<String, ArrayList<String>> map,
                                                  final String[] splittedGroups) {
    if (splittedGroups.length == 1 && splittedGroups[0].matches("\\D+")) {
      return Arrays.asList(splittedGroups);
    }
    return Arrays.stream(splittedGroups).flatMap(group -> {
      if (group.matches("\\d+")) {
        return groupsToListOfNodes(map, map.get(group).get(0).split("\\+")).stream();
      } else {
        return Stream.of(group);
      }
    }).collect(Collectors.toList());
  }
}
