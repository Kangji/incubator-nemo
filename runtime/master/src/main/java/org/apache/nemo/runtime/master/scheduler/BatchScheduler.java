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

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.Vertex;
import org.apache.nemo.common.exception.UnknownExecutionStateException;
import org.apache.nemo.common.exception.UnrecoverableFailureException;
import org.apache.nemo.common.ir.vertex.executionproperty.ClonedSchedulingProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.common.state.StageState;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.PlanAppender;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.resource.DefaultExecutorRepresenter;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * (CONCURRENCY) Only a single dedicated thread should use the public methods of this class.
 * (i.e., runtimeMasterThread in RuntimeMaster)
 * <p>
 * BatchScheduler receives a single {@link PhysicalPlan} to execute and schedules the Tasks.
 *
 * Note: When modifying this class, take a look at {@link SimulationScheduler}.
 */
@DriverSide
@NotThreadSafe
public final class BatchScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(BatchScheduler.class.getName());

  /**
   * Run-time optimizations.
   */
  private final PlanRewriter planRewriter;

  /**
   * Components related to scheduling the given plan.
   */
  private final TaskDispatcher taskDispatcher;  // Class for dispatching tasks.
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;  // A 'pointer' to the list of pending tasks.
  private final ExecutorRegistry executorRegistry;  // A registry for executors available for the job.
  private final PlanStateManager planStateManager;  // A component that manages the state of the plan.

  /**
   * Other necessary components of this {@link org.apache.nemo.runtime.master.RuntimeMaster}.
   */
  private final BlockManagerMaster blockManagerMaster;  // A component that manages data blocks.

  /**
   * The below variables depend on the submitted plan to execute.
   */
  private List<List<Stage>> sortedScheduleGroups;  // Stages, sorted in the order to be scheduled.
  // Stores the partitionSize counted per key of the task output of each stage.
  private final Map<String, Map<Integer, Long>> stageIdToOutputPartitionSizeMap = new HashMap<>();

  @Inject
  private BatchScheduler(final PlanRewriter planRewriter,
                         final TaskDispatcher taskDispatcher,
                         final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                         final BlockManagerMaster blockManagerMaster,
                         final ExecutorRegistry executorRegistry,
                         final PlanStateManager planStateManager) {
    this.planRewriter = planRewriter;
    this.taskDispatcher = taskDispatcher;
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.blockManagerMaster = blockManagerMaster;
    this.executorRegistry = executorRegistry;
    this.planStateManager = planStateManager;
  }

  ////////////////////////////////////////////////////////////////////// Methods for plan rewriting.

  @Override
  public void updatePlan(final PhysicalPlan newPhysicalPlan) {
    // update the physical plan in the scheduler.
    // NOTE: what's already been executed is not modified in the new physical plan.
    // TODO #182: Consider reshaping in run-time optimization. At now, we only consider plan appending.
    updatePlan(newPhysicalPlan, planStateManager.getMaxScheduleAttempt());
  }

  /**
   * Update the physical plan in the scheduler.
   *
   * @param newPhysicalPlan    the new physical plan to update.
   * @param maxScheduleAttempt the maximum number of task scheduling attempt.
   */
  private void updatePlan(final PhysicalPlan newPhysicalPlan,
                          final int maxScheduleAttempt) {
    planStateManager.updatePlan(newPhysicalPlan, maxScheduleAttempt);
    this.sortedScheduleGroups = newPhysicalPlan.getStageDAG().getVertices().stream()
      .collect(Collectors.groupingBy(Stage::getScheduleGroup))
      .entrySet().stream()
      .sorted(Map.Entry.comparingByKey())
      .map(Map.Entry::getValue)
      .collect(Collectors.toList());
  }

  /**
   * Process the RuntimePassMessage.
   *
   * @param taskId           that generated the message.
   * @param data             of the message.
   */
  public void onRunTimePassMessage(final String taskId, final Object data) {
    BatchSchedulerUtils.onRunTimePassMessage(planStateManager, planRewriter, taskId, data);
  }

  ////////////////////////////////////////////////////////////////////// Methods for scheduling.

  /**
   * Schedules a given plan.
   * If multiple physical plans are submitted, they will be appended and handled as a single plan.
   * TODO #182: Consider reshaping in run-time optimization. At now, we only consider plan appending.
   *
   * @param submittedPhysicalPlan the physical plan to schedule.
   * @param maxScheduleAttempt    the max number of times this plan/sub-part of the plan should be attempted.
   */
  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan,
                           final int maxScheduleAttempt) {
    LOG.info("Plan to schedule: {}", submittedPhysicalPlan.getPlanId());

    if (!planStateManager.isInitialized()) {
      // First scheduling.
      taskDispatcher.run();
      updatePlan(submittedPhysicalPlan, maxScheduleAttempt);
      planStateManager.storeJSON("submitted");
    } else {
      // Append the submitted plan to the original plan.
      final PhysicalPlan appendedPlan =
        PlanAppender.appendPlan(planStateManager.getPhysicalPlan(), submittedPhysicalPlan);
      updatePlan(appendedPlan, maxScheduleAttempt);
      planStateManager.storeJSON("appended");
    }

    doSchedule();
  }

  /**
   * Handles task state transition notifications sent from executors.
   * Note that we can receive notifications for previous task attempts, due to the nature of asynchronous events.
   * We ignore such late-arriving notifications, and only handle notifications for the current task attempt.
   *
   * @param executorId       the id of the executor where the message was sent from.
   * @param taskId           whose state has changed
   * @param taskAttemptIndex of the task whose state has changed
   * @param newState         the state to change to
   * @param vertexPutOnHold  the ID of vertex that is put on hold. It is null otherwise.
   */
  @Override
  public void onTaskStateReportFromExecutor(final String executorId,
                                            final String taskId,
                                            final int taskAttemptIndex,
                                            final TaskState.State newState,
                                            @Nullable final String vertexPutOnHold,
                                            final TaskState.RecoverableTaskFailureCause failureCause) {
    // Do change state, as this notification is for the current task attempt.
    planStateManager.onTaskStateChanged(taskId, newState);
    switch (newState) {
      case COMPLETE:
        BatchSchedulerUtils.onTaskExecutionComplete(executorRegistry, executorId, taskId);
        break;
      case SHOULD_RETRY:
        // SHOULD_RETRY from an executor means that the task ran into a recoverable failure
        BatchSchedulerUtils.onTaskExecutionFailedRecoverable(planStateManager, blockManagerMaster, executorRegistry,
          executorId, taskId, failureCause);
        break;
      case ON_HOLD:
        final Optional<PhysicalPlan> optionalPhysicalPlan =
          BatchSchedulerUtils
            .onTaskExecutionOnHold(planStateManager, executorRegistry, planRewriter, executorId, taskId);
        optionalPhysicalPlan.ifPresent(this::updatePlan);
        break;
      case FAILED:
        throw new UnrecoverableFailureException(new Exception(String.format("The plan failed on %s in %s",
          taskId, executorId)));
      case READY:
      case EXECUTING:
        throw new RuntimeException("The states READY/EXECUTING cannot occur at this point");
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + newState));
    }

    // Invoke doSchedule()
    switch (newState) {
      case COMPLETE:
      case ON_HOLD:
        if (true) {
          final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
          if (checkForWorkStealingBaseConditions(stageId)) {
            // need to have WorkStealingManager? or WorkStealingUtils?
            // first, need to figure out currently running task ids
            // second, track their size by task index
            // third, track their currently processed data in bytes
            // fourth, track their execution time till now
            // five, detect skew!
            final int dummyfile = 1;
          }
        }
        // If the stage has completed
        final String stageIdForTaskUponCompletion = RuntimeIdManager.getStageIdFromTaskId(taskId);
        if (planStateManager.getStageState(stageIdForTaskUponCompletion).equals(StageState.State.COMPLETE)
          && !planStateManager.isPlanDone()) {
          doSchedule();
        }
        break;
      case SHOULD_RETRY:
        // Do retry
        doSchedule();
        break;
      default:
        break;
    }

    // Invoke taskDispatcher.onExecutorSlotAvailable()
    switch (newState) {
      // These three states mean that a slot is made available.
      case COMPLETE:
      case ON_HOLD:
      case SHOULD_RETRY:
        taskDispatcher.onExecutorSlotAvailable();
        break;
      default:
        break;
    }
  }

  @Override
  public void onSpeculativeExecutionCheck() {
    MutableBoolean isNewCloneCreated = new MutableBoolean(false);

    BatchSchedulerUtils.selectEarliestSchedulableGroup(sortedScheduleGroups, planStateManager)
      .ifPresent(scheduleGroup ->
        scheduleGroup.stream().map(Stage::getId).forEach(stageId -> {
          final Stage stage = planStateManager.getPhysicalPlan().getStageDAG().getVertexById(stageId);

          // Only if the ClonedSchedulingProperty is set...
          stage.getPropertyValue(ClonedSchedulingProperty.class).ifPresent(cloneConf -> {
            if (!cloneConf.isUpFrontCloning()) { // Upfront cloning is already handled.
              isNewCloneCreated.setValue(doSpeculativeExecution(stage, cloneConf));
            }
          });
        }));

    if (isNewCloneCreated.booleanValue()) {
      doSchedule(); // Do schedule the new clone.
    }
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    LOG.info("{} added (node: {})", executorRepresenter.getExecutorId(), executorRepresenter.getNodeName());
    executorRegistry.registerExecutor(executorRepresenter);
    taskDispatcher.onExecutorSlotAvailable();
  }

  @Override
  public void onExecutorRemoved(final String executorId) {
    LOG.info("{} removed", executorId);
    blockManagerMaster.removeWorker(executorId);

    // These are tasks that were running at the time of executor removal.
    final Set<String> interruptedTasks = new HashSet<>();
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      interruptedTasks.addAll(executor.onExecutorFailed());
      return Pair.of(executor, ExecutorRegistry.ExecutorState.FAILED);
    });

    // Blocks of the interrupted tasks are failed.
    interruptedTasks.forEach(blockManagerMaster::onProducerTaskFailed);

    // Retry the interrupted tasks (and required parents)
    BatchSchedulerUtils.retryTasksAndRequiredParents(planStateManager, blockManagerMaster, interruptedTasks);

    // Trigger the scheduling of SHOULD_RETRY tasks in the earliest scheduleGroup
    doSchedule();
  }

  @Override
  public void terminate() {
    this.taskDispatcher.terminate();
    this.executorRegistry.terminate();
  }

  ////////////////////////////////////////////////////////////////////// Task launch methods.

  /**
   * The main entry point for task scheduling.
   * This operation can be invoked at any point during job execution, as it is designed to be free of side-effects.
   * <p>
   * These are the reasons why.
   * - We 'reset' {@link PendingTaskCollectionPointer}, and not 'add' new tasks to it
   * - We make {@link TaskDispatcher} dispatch only the tasks that are READY.
   */
  private void doSchedule() {
    final Optional<List<Stage>> earliest =
      BatchSchedulerUtils.selectEarliestSchedulableGroup(sortedScheduleGroups, planStateManager);

    if (earliest.isPresent()) {
      final List<Task> tasksToSchedule = earliest.get().stream()
        .flatMap(stage ->
          BatchSchedulerUtils.selectSchedulableTasks(planStateManager, blockManagerMaster, stage).stream())
        .collect(Collectors.toList());
      if (!tasksToSchedule.isEmpty()) {
        LOG.info("Scheduling some tasks in {}, which are in the same ScheduleGroup", tasksToSchedule.stream()
          .map(Task::getTaskId)
          .map(RuntimeIdManager::getStageIdFromTaskId)
          .collect(Collectors.toSet()));

        // Set the pointer to the schedulable tasks.
        pendingTaskCollectionPointer.setToOverwrite(tasksToSchedule);

        // Notify the dispatcher that a new collection is available.
        taskDispatcher.onNewPendingTaskCollectionAvailable();
      }
    } else {
      LOG.info("Skipping this round as no ScheduleGroup is schedulable.");
    }
  }


  ////////////////////////////////////////////////////////////////////// Task cloning methods.

  /**
   * @return true if a new clone is created.
   *         false otherwise.
   */
  private boolean doSpeculativeExecution(final Stage stage, final ClonedSchedulingProperty.CloneConf cloneConf) {
    final double fractionToWaitFor = cloneConf.getFractionToWaitFor();
    final Object[] completedTaskTimes = planStateManager.getCompletedTaskTimeListMs(stage.getId()).toArray();

    // Only after the fraction of the tasks are done...
    // Delayed cloning (aggressive)
    if (completedTaskTimes.length > 0
      && completedTaskTimes.length >= Math.round(stage.getTaskIndices().size() * fractionToWaitFor)) {
      // Only if the running task is considered a 'straggler'....
      Arrays.sort(completedTaskTimes);
      final long medianTime = (long) completedTaskTimes[completedTaskTimes.length / 2];
      final double medianTimeMultiplier = cloneConf.getMedianTimeMultiplier();
      final Map<String, Long> executingTaskToTime = planStateManager.getExecutingTaskToRunningTimeMs(stage.getId());

      return modifyStageNumCloneUsingMedianTime(
        stage.getId(), completedTaskTimes.length, medianTime, medianTimeMultiplier, executingTaskToTime);
    } else {
      return false;
    }
  }

  /**
   * @return true if the number of clones for the stage is modified.
   *         false otherwise.
   */
  private boolean modifyStageNumCloneUsingMedianTime(final String stageId,
                                                     final long numCompletedTasks,
                                                     final long medianTime,
                                                     final double medianTimeMultiplier,
                                                     final Map<String, Long> executingTaskToTime) {
    for (final Map.Entry<String, Long> entry : executingTaskToTime.entrySet()) {
      final long runningTime = entry.getValue();
      if (runningTime > Math.round(medianTime * medianTimeMultiplier)) {
        final String taskId = entry.getKey();
        final boolean isNumCloneModified = planStateManager
          .setNumOfClones(stageId, RuntimeIdManager.getIndexFromTaskId(taskId), 2);
        if (isNumCloneModified) {
          LOG.info("Cloned {}, because its running time {} (ms) is bigger than {} tasks' "
              + "(median) {} (ms) * (multiplier) {}", taskId, runningTime, numCompletedTasks,
            medianTime, medianTimeMultiplier);
          return true;
        }
      }
    }

    return false;
  }

  // 일단은 다 여기다 만들고, 나중에 BatchSchedulerUtils 든 어디든 예쁘게 구분해서 정리하기
  public void aggregateStageIdToPartitionSizeMap(final String taskId,
                                                 final Map<Integer, Long> partitionSizeMap) {
    final Map<Integer, Long> partitionSizeMapForThisStage = stageIdToOutputPartitionSizeMap
      .get(RuntimeIdManager.getStageIdFromTaskId(taskId));
    for (Integer hashedKey : partitionSizeMap.keySet()) {
      final Long partitionSize = partitionSizeMap.get(hashedKey);
      if (partitionSizeMapForThisStage.containsKey(hashedKey)) {
        partitionSizeMapForThisStage.compute(hashedKey, (existingKey, existingValue) -> existingValue + partitionSize);
      } else {
        partitionSizeMapForThisStage.put(hashedKey, partitionSize);
      }
    }
  }

  private boolean checkForWorkStealingBaseConditions(final String stageId) {
    final boolean executorStatus = executorRegistry.isExecutorSlotAvailable();
    final int totalNumberOfSlots = executorRegistry.getTotalNumberOfExecutorSlots();
    final int remainingTasks = planStateManager.getNumberOfTasksRemainingInStage(stageId);
    return executorStatus && (totalNumberOfSlots > remainingTasks);
  }

  private Set<String> getCurrentlyRunningTaskId(final String stageId) {
    return planStateManager.getOngoingTaskIdsInStage(stageId);
  }

  private Set<String> getStagesOfPreviousSchedulingGroup(final String currentStageId) {
    return planStateManager.getPhysicalPlan().getStageDAG().getParents(currentStageId).stream()
      .map(Vertex::getId)
      .collect(Collectors.toSet());
  }

  // key가 정말로 Hash된 값으로 들어가는지(아님, 지금은 block의 경우 전부 다 디폴트 키가 integer라서 이런 식으로 된 것),
  // block.write() 에서 partitioner를 가지고 작업 - hash 되었다고 생각할 수 있을 듯. 일단 한번 해보고 안 되면 고치자
  // 그리고 2개 이상의 stage가 parent로 함께 있을 때 child 입장에서 partition이 올바르게 나누어져 있는지 꼭 확인하기
  private Map<String, Long> getInputSizesOfCurrentlyRunningTaskIds(final Set<String> parentStageIds,
                                                                    final Set<String> currentlyRunningTaskIds) {
    // key를 hashed key로 바꿔야 함
    // 그 다음에 task 별로 input size 알려주기
    Map<String, Long> currentlyRunningTaskIdsToTotalSize = new HashMap<>();
    for (String parent : parentStageIds) {
      Map<Integer, Long> taskIdxToSize = stageIdToOutputPartitionSizeMap.get(parent);
      for (String taskId : currentlyRunningTaskIds) {
        if (currentlyRunningTaskIdsToTotalSize.containsKey(taskId)) {
          final long existingValue = currentlyRunningTaskIdsToTotalSize.get(taskId);
          currentlyRunningTaskIdsToTotalSize.put(taskId,
            existingValue + taskIdxToSize.get(RuntimeIdManager.getIndexFromTaskId(taskId)));
        } else {
          currentlyRunningTaskIdsToTotalSize
            .put(taskId, taskIdxToSize.get(RuntimeIdManager.getIndexFromTaskId(taskId)));
        }
      }
    }
    return currentlyRunningTaskIdsToTotalSize;
  }



  private Map<String, Long> getCurrentlyProcessedBytesOfRunningTasks() {
    // driver sends message to executors
    executorRegistry.viewExecutors(executors -> executors.forEach(executor -> executor.sendControlMessage()));
    // executor sends back the requested information
    // manage that information in here
    return new HashMap<>();
  }

  private Map<String, Long> getCurrentlyTakenExecutionTimeMsOfRunningTasks(final String stageId) {
    return planStateManager.getExecutingTaskToRunningTimeMs(stageId);
  }

  private void detectSkew(final String stageId) {
    // get size of running tasks
    Set<String> currentlyRunningTaskIds = getCurrentlyRunningTaskId(stageId); // 이것도 아이디라고 하면 좋을 것 같은데
    Set<String> parentStageId = getStagesOfPreviousSchedulingGroup(stageId);
    Map<String, Long> inputSizeOfCandidateTasks =
      getInputSizesOfCurrentlyRunningTaskIds(parentStageId, currentlyRunningTaskIds);

    // get size of processed bytes of running tasks
    Map<String, Long> taskIdToProcessedBytes = getCurrentlyProcessedBytesOfRunningTasks();
    // get time taken to process the bytes
    Map<String, Long> taskIdToElapsedTime = getCurrentlyTakenExecutionTimeMsOfRunningTasks(stageId);

    // estimate the remaining time
    List<Pair<String, Long>> estimatedTimeToFinishPerTask = new ArrayList<>(taskIdToElapsedTime.size());

    // detect skew


    // Need to think about
    // 1. taskId가 아니라 task index정보만 가지고 있어도 충분한가? fail -> retry 의 과정을 거칠 테니 attempt만 다른 task 2개가
    // 동시에 돌아갈 일은 거의 없다고 봐도 되겠지?
    // 2. stage 기반이 아니라 scheduling group 기반으로 되어야 하는 것 아닌가?
    // (이러면 구현해 놓은 코드 좀 많이 고쳐야 할 것 같긴 한데, 이게 더 맞는 방향인 것 같기도 하고, 잘 모르겠네)
  }
}
