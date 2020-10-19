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
    final OptionalInt minOccupancy =
      executors.stream()
        .mapToInt(ExecutorRepresenter::getNumOfRunningTasks)
        .min();
    final OptionalDouble avgOccupancy =
      executors.stream()
        .mapToInt(ExecutorRepresenter::getNumOfRunningTasks)
        .average();

    if (!minOccupancy.isPresent() || !avgOccupancy.isPresent()) {
      throw new SchedulingException("Cannot find min/max occupancy");
    }

    if (pipeManagerMaster.isActive()) {
      LOG.error("Using PipeManagerMaster!!");
      final List<String> dataLocationExecutorNodeNames = task.getTaskIncomingEdges().stream()
        .map(e -> pipeManagerMaster.getLocation(e.getId(), RuntimeIdManager.getIndexFromTaskId(task.getTaskId())))
        .map(registry::executorIdToNodeName)
        .collect(Collectors.toList());

      return candidateDataLocation(dataLocationExecutorNodeNames, executors, avgOccupancy.getAsDouble());

    } else {
      return executors.stream()
        .filter(executor -> executor.getNumOfRunningTasks() == minOccupancy.getAsInt())
        .findFirst()
        .orElseThrow(() -> new SchedulingException("No such executor"));
    }
  }

  /**
   * Extract the candidate executors from the source data locations of the intermediate data.
   * @param dataSourceExecutors source executors.
   * @return
   */
  private static ExecutorRepresenter candidateDataLocation(final List<String> dataSourceExecutors,
                                                           final Collection<ExecutorRepresenter> executors,
                                                           final Double avgOccupancy) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      final Map<String, String> map = mapper.readValue(
        new File(Util.fetchProjectRootPath() + "/bin/labeldict.json"), Map.class);

      final List<List<String>> list = new ArrayList<>();
      map.forEach((k, v) -> list.add(Arrays.asList(v.split("\\+"))));

      for (final List<String> candidates: list) {
        LOG.error("checking for {} in {}", dataSourceExecutors, candidates);
        if (candidates.containsAll(dataSourceExecutors) && executors.stream()
          .filter(e -> candidates.contains(e.getNodeName()))
          .anyMatch(e -> e.getNumOfRunningTasks() < avgOccupancy)) {
          return executors.stream()
            .filter(executor -> candidates.contains(executor.getNodeName()))
            .min(Comparator.comparingInt(ExecutorRepresenter::getNumOfRunningTasks))
            .orElseThrow(() -> new RuntimeException("No such executor"));
        }
      }

      // Just return the one with the min occupancy.
      return executors.stream()
        .min(Comparator.comparingInt(ExecutorRepresenter::getNumOfRunningTasks))
        .orElseThrow(() -> new RuntimeException("No such executor"));
    } catch (final Exception e) {
      throw new SchedulingException(e);
    }
  }
}
