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
import java.util.stream.Stream;

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

  @Inject
  @VisibleForTesting
  public RoundRobinSchedulingPolicy(final ExecutorRegistry executorRegistry) {
    this.executorRegistry = executorRegistry;
  }

  @Override
  public boolean scheduleTaskGroup(final ScheduledTaskGroup scheduledTaskGroup,
                                   final JobStateManager jobStateManager) {
    final String containerType = scheduledTaskGroup.getContainerType();

    Optional<String> executorId = selectExecutorByRR(containerType, scheduledTaskGroup.getLocation());
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
   * @param location location criterion for the executor, or {@code null}
   * @return (optionally) the selected executor.
   */
  private Optional<String> selectExecutorByRR(final String containerType, final String location) {
    final Stream<ExecutorRepresenter> executors = executorRegistry.getRunningExecutorIds().stream()
        .map(id -> executorRegistry.getRunningExecutorRepresenter(id))
        .filter(RoundRobinSchedulingPolicy::hasFreeSlot);
    final Stream<ExecutorRepresenter> executorsWithMatchingContainerType =
        (containerType.equals(ExecutorPlacementProperty.NONE)) ? executors
        : executors.filter(executor -> executor.getContainerType().equals(containerType));
    final List<ExecutorRepresenter> candidateExecutors = (location == null ? executorsWithMatchingContainerType
        : executorsWithMatchingContainerType.filter(executor -> executor.getNodeName().equals(location)))
        .collect(Collectors.toList());
    final OptionalInt minOccupancy = candidateExecutors.stream()
        .map(executor -> executor.getRunningTaskGroups().size()).mapToInt(i -> i).min();

    if (minOccupancy.isPresent()) {
      final ExecutorRepresenter chosenExecutor = candidateExecutors.stream()
          .filter(executor -> executor.getRunningTaskGroups().size() == minOccupancy.getAsInt())
          .findFirst().get();
      return Optional.of(chosenExecutor.getExecutorId());
    } else {
      return Optional.empty();
    }
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
    //LOG.debug("Has Free Slot: " + executor.getExecutorId());
    //LOG.debug("Running TaskGroups: " + executor.getRunningTaskGroups());
    return executor.getRunningTaskGroups().size() - executor.getSmallTaskGroups().size()
        < executor.getExecutorCapacity();
  }
}
