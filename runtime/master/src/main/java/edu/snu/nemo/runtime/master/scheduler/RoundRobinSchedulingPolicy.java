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
package edu.snu.nemo.runtime.master.scheduler;

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

/**
 * {@inheritDoc}
 * A Round-Robin implementation used by {@link BatchSingleJobScheduler}.
 *
 * This policy keeps a list of available {@link ExecutorRepresenter} for each type of container.
 * The RR policy is used for each container type when trying to schedule a task group.
 */
@ThreadSafe
@DriverSide
public final class RoundRobinSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(RoundRobinSchedulingPolicy.class.getName());

  private final ExecutorRegistry executorRegistry;
  private final Map<String, Integer> executorIdToHeavyTaskGroupMap;

  @Inject
  @VisibleForTesting
  public RoundRobinSchedulingPolicy(final ExecutorRegistry executorRegistry) {
    this.executorRegistry = executorRegistry;
    this.executorIdToHeavyTaskGroupMap = new HashMap<>();
  }

  @Override
  public boolean scheduleTaskGroup(final ScheduledTaskGroup scheduledTaskGroup,
                                   final JobStateManager jobStateManager) {
    final String containerType = scheduledTaskGroup.getContainerType();

    Optional<String> executorId = considerSkew(scheduledTaskGroup, containerType);
    if (!executorId.isPresent()) { // If there is no available executor to schedule this task group now,
      return false;
    } else {
      scheduleTaskGroup(executorId.get(), scheduledTaskGroup, jobStateManager);
      return true;
    }
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executor) {
  }

  @Override
  public Set<String> onExecutorRemoved(final String executorId) {
    final ExecutorRepresenter executor = executorRegistry.getFailedExecutorRepresenter(executorId);
    return Collections.unmodifiableSet(executor.getFailedTaskGroups());
  }

  @Override
  public void onTaskGroupExecutionComplete(final String executorId, final String taskGroupId) {
  }

  @Override
  public void onTaskGroupExecutionFailed(final String executorId, final String taskGroupId) {
  }

  @Override
  public void terminate() {
  }

  /**
   * Sticks to the RR policy to select an executor for the next task group.
   * It checks the task groups running (as compared to each executor's capacity).
   *
   * @param containerType to select an executor for.
   * @return (optionally) the selected executor.
   */
  private Optional<String> selectExecutorByRR(final String containerType) {
    final List<ExecutorRepresenter> executors = executorRegistry.getRunningExecutorIds().stream()
        .map(id -> executorRegistry.getRunningExecutorRepresenter(id))
        .filter(RoundRobinSchedulingPolicy::hasFreeSlot)
        .collect(Collectors.toList());

    final List<ExecutorRepresenter> candidateExecutors = containerType.equals(ExecutorPlacementProperty.NONE)
        ? executors
        : executors.stream().filter(executor -> executor.getContainerType().equals(containerType))
        .collect(Collectors.toList());
    Optional<ExecutorRepresenter> executorWithMinOccupancy = candidateExecutors.stream()
        .min(Comparator.comparingInt(executor -> executor.getRunningTaskGroups().size()));
    if (executorWithMinOccupancy.isPresent()) {
      LOG.info("MinOccupancy: {}/8", executorWithMinOccupancy.get().getRunningTaskGroups().size());
      final ExecutorRepresenter chosenExecutor = executorWithMinOccupancy.get();
      return Optional.of(chosenExecutor.getExecutorId());
    } else {
      return Optional.empty();
    }
  }

  private Optional<String> considerSkew(final ScheduledTaskGroup scheduledTaskGroup,
                                              final String containerType) {
    // Check if the scheduledTaskGroup has hot key data
    int scheduledTaskGroupIdx = scheduledTaskGroup.getTaskGroupIdx();
    boolean isHotHash = false;
    for (int i = 0; i < scheduledTaskGroup.getTaskGroupIncomingEdges().size(); i++) {
      PhysicalStageEdge edge = scheduledTaskGroup.getTaskGroupIncomingEdges().get(i);
      if (edge.getTaskGroupIdxToKeyRange().get(scheduledTaskGroupIdx).right()) {
        LOG.info("{} has Hot Hash", scheduledTaskGroupIdx);
        isHotHash = true;
        break;
      }
    }

    if (isHotHash) {
      final List<ExecutorRepresenter> executors = executorRegistry.getRunningExecutorIds().stream()
          .map(id -> executorRegistry.getRunningExecutorRepresenter(id))
          .filter(RoundRobinSchedulingPolicy::hasFreeSlot)
          .collect(Collectors.toList());

      final List<ExecutorRepresenter> candidateExecutors = containerType.equals(ExecutorPlacementProperty.NONE)
          ? executors
          : executors.stream().filter(executor -> executor.getContainerType().equals(containerType))
          .collect(Collectors.toList());

      Set<String> heavyExecutorIds = executorIdToHeavyTaskGroupMap.keySet();
      List<ExecutorRepresenter> lightExecutors = candidateExecutors.stream()
          .filter(lightex -> !heavyExecutorIds.contains(lightex.getExecutorId()))
          .collect(Collectors.toList());

      LOG.info("Hot Hash: Candidates {} heavyEx {} lightEx {}", candidateExecutors.size(),
          heavyExecutorIds.size(), lightExecutors.size());

      if (!lightExecutors.isEmpty()) {
        final String selectedExecutorId = lightExecutors.get(0).getExecutorId();
        executorIdToHeavyTaskGroupMap.put(selectedExecutorId, scheduledTaskGroupIdx);
        LOG.info("Hot Hash: {} TaskGroup {}", selectedExecutorId, scheduledTaskGroupIdx);
        return Optional.of(selectedExecutorId);
      } else {
        LOG.info("Hot Hash: Couldn't schedule {}: no free slot in light exs", scheduledTaskGroupIdx);
        return Optional.empty();
      }
    }

    return selectExecutorByRR(containerType);
  }

  /**
   * Schedules and sends a TaskGroup to the given executor.
   *
   * @param executorId         of the executor to execute the TaskGroup.
   * @param scheduledTaskGroup to assign.
   * @param jobStateManager    which the TaskGroup belongs to.
   */
  private void scheduleTaskGroup(final String executorId,
                                 final ScheduledTaskGroup scheduledTaskGroup,
                                 final JobStateManager jobStateManager) {
    jobStateManager.onTaskGroupStateChanged(scheduledTaskGroup.getTaskGroupId(), TaskGroupState.State.EXECUTING);

    final ExecutorRepresenter executor = executorRegistry.getRunningExecutorRepresenter(executorId);
    LOG.info("Scheduling {} to {}({})",
        new Object[]{scheduledTaskGroup.getTaskGroupId(), executorId, executor.getNodeName()});
    executor.onTaskGroupScheduled(scheduledTaskGroup);
  }

  private static boolean hasFreeSlot(final ExecutorRepresenter executor) {
    LOG.info("{} Cores: {} occupied among {}", executor.getExecutorId(),
        executor.getRunningTaskGroups().size() - executor.getSmallTaskGroups().size(),
        executor.getExecutorCapacity());
    return executor.getRunningTaskGroups().size() - executor.getSmallTaskGroups().size()
        < executor.getExecutorCapacity();
  }
}
