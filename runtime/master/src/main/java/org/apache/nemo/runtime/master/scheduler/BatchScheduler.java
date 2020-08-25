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

import com.google.protobuf.ByteString;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.Vertex;
import org.apache.nemo.common.exception.UnknownExecutionStateException;
import org.apache.nemo.common.exception.UnrecoverableFailureException;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.executionproperty.ClonedSchedulingProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.metric.TaskMetric;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.common.state.StageState;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.PlanAppender;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final MetricStore metricStore = MetricStore.getStore();

  /**
   * Other necessary components of this {@link org.apache.nemo.runtime.master.RuntimeMaster}.
   */
  private final BlockManagerMaster blockManagerMaster;  // A component that manages data blocks.

  /**
   * The below variables depend on the submitted plan to execute.
   */
  private List<List<Stage>> sortedScheduleGroups;  // Stages, sorted in the order to be scheduled.


  // Stores the partitionSize counted per key of the task output of each stage.
  private final Set<String> workStealingCandidates = new HashSet<>();
  private final Map<String, Map<Integer, Long>> stageIdToOutputPartitionSizeMap = new HashMap<>();
  private final Map<String, Long> taskIdToProcessedBytes = new HashMap<>();
  private final Map<String, Boolean> stageIdToWorkStealingExecuted = new HashMap<>();

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
    for (Stage stage : planStateManager.getPhysicalPlan().getStageDAG().getVertices()) {
      stageIdToWorkStealingExecuted.putIfAbsent(stage.getId(), false);
    }
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
  public void onWorkStealingCheck() {
    MutableBoolean isWorkStealingConditionSatisfied = new MutableBoolean(false);
    List<Stage> scheduleGroup = BatchSchedulerUtils
      .selectEarliestSchedulableGroup(sortedScheduleGroups, planStateManager).orElse(new ArrayList<>());
    List<String> scheduleGroupInId = scheduleGroup.stream().map(Stage::getId).collect(Collectors.toList());
    isWorkStealingConditionSatisfied.setValue(checkForWorkStealingBaseConditions(scheduleGroupInId));

    if (isWorkStealingConditionSatisfied.booleanValue()) {
      LOG.error("schedule group: {}", scheduleGroupInId);
      taskIdToProcessedBytes.clear();

      final Map<String, Pair<Integer, Integer>> skewedTasks = detectSkew(scheduleGroupInId);
      if (skewedTasks.isEmpty()) {
        return;
      }

      // skewed tasks are generated. Now, split tasks
      final Map<String, Integer> stageToNumOfTasks = new HashMap<>();
      for (String taskId : skewedTasks.keySet()) {
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
        if (!stageToNumOfTasks.containsKey(stageId)) {
          stageToNumOfTasks.put(stageId, 1);
        } else {
          stageToNumOfTasks.computeIfPresent(stageId, (existingKey, value) -> value + 1);
        }
      }

      List<Task> tasksToSchedule = new ArrayList<>(skewedTasks.size());
      // hold updated iterator information
      // string: taskId(original)
      // pair.left() : iterator starting index: always 0
      // pair.right() : iterator ending index: Integer.MAX_INT if non skewed task
      Map<String, Pair<Integer, Integer>> accumulatedResult = new HashMap<>();

      // tasks are generated in "stage" based : loop on stages, not schedule group
      for (String stageId : stageToNumOfTasks.keySet()) {

        Stage stageToSchedule = planStateManager.getPhysicalPlan().getStageDAG().getVertexById(stageId);

        // make new task ids and store that information in corresponding stage and planstatemanager
        // for now, id logic for robber tasks are as follows:
          // - same stage id (obvious)
          // - same index number (need to fetch the same data as the victim task)
          // - attempt number is replaced with "*", similar withe the block wildcard id.

        //generate the robber tasks' id
        final Set<String> idsForRobberTasks = new HashSet<>();
        final Set<String> idsForVictimTasks = new HashSet<>();
        for (String taskId : skewedTasks.keySet()) {
          if (taskId.contains(stageId)) {
            idsForVictimTasks.add(taskId);
            idsForRobberTasks.add(RuntimeIdManager.generateWorkStealingTaskId(taskId));
          }
        }

        // update the work stealing tasks in Stage and PlanStateManager
        planStateManager.getPhysicalPlan().getStageDAG()
          .getVertexById(stageId).setWorkStealingTaskIds(idsForRobberTasks);
        planStateManager.addWorkStealingTasks(idsForRobberTasks);

        // house keeping stuffs needed for initializing tasks
        // create and return Robber tasks
        final List<StageEdge> stageIncomingEdges =
          planStateManager.getPhysicalPlan().getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
        final List<StageEdge> stageOutgoingEdges =
          planStateManager.getPhysicalPlan().getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());
        final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();

        idsForVictimTasks.forEach(taskId -> {
          final Set<String> blockIds = BatchSchedulerUtils.getOutputBlockIds(planStateManager, taskId);
          blockManagerMaster.onProducerTaskScheduled(taskId, blockIds);
          final int taskIdx = RuntimeIdManager.getIndexFromTaskId(taskId);
          final Pair<Integer, Integer> currentAndTotalIteratorIndex = skewedTasks.get(taskId);
          // calculate the starting index of Robber task
          final int iteratorStartingIndex = (int) Math.floor(
            ((currentAndTotalIteratorIndex.right() + currentAndTotalIteratorIndex.left()) / 2) + 1);
          LOG.error("{} iterator starting index: {}", taskId, iteratorStartingIndex);
          tasksToSchedule.add(new Task(
            planStateManager.getPhysicalPlan().getPlanId(),
            RuntimeIdManager.generateWorkStealingTaskId(taskId),
            stageToSchedule.getExecutionProperties(),
            stageToSchedule.getSerializedIRDAG(),
            stageIncomingEdges,
            stageOutgoingEdges,
            vertexIdToReadables.get(taskIdx),
            new AtomicInteger(iteratorStartingIndex), // index for robber tasks
            new AtomicInteger(currentAndTotalIteratorIndex.right())));
        });

        // accumulate the Victim tasks and non skewed tasks result
        for (String taskId : workStealingCandidates) {
          if (idsForVictimTasks.contains(taskId)) { // this is for skewed task
            Pair<Integer, Integer> currentAndTotalIteratorIndex = skewedTasks.get(taskId);
            accumulatedResult.put(taskId,
              Pair.of(0, (int) Math.floor(
                ((currentAndTotalIteratorIndex.right() + currentAndTotalIteratorIndex.left()) / 2) + 1)));
          } else { // this is for non skewed tasks
            accumulatedResult.put(taskId, Pair.of(0, Integer.MAX_VALUE));
          }
        }
        // do work stealing for only once : this is because of the index based task state tracking system
        // Need to be handled in the near future!
        stageIdToWorkStealingExecuted.put(stageId, true);
      }
      // notify the updated information to executors
      LOG.error("accumulated result: {}", accumulatedResult);
      sendWorkStealingResultToExecutor(accumulatedResult);

      // schedule new tasks
      pendingTaskCollectionPointer.setToOverwrite(tasksToSchedule);
      taskDispatcher.onNewPendingTaskCollectionAvailable();
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
    taskIdToProcessedBytes.clear();
    workStealingCandidates.clear();
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
      .getOrDefault(RuntimeIdManager.getStageIdFromTaskId(taskId), new HashMap<>());
    for (Integer hashedKey : partitionSizeMap.keySet()) {
      final Long partitionSize = partitionSizeMap.get(hashedKey);
      if (partitionSizeMapForThisStage.containsKey(hashedKey)) {
        partitionSizeMapForThisStage.put(hashedKey, partitionSize + partitionSizeMapForThisStage.get(hashedKey));
      } else {
        partitionSizeMapForThisStage.put(hashedKey, partitionSize);
      }
    }
    stageIdToOutputPartitionSizeMap.put(RuntimeIdManager.getStageIdFromTaskId(taskId), partitionSizeMapForThisStage);
  }

  public void aggregateTaskIdToProcessedBytes(final String taskId,
                                              final long processedBytes) {
    LOG.error("{} data arrived! {}", taskId, processedBytes);
    taskIdToProcessedBytes.put(taskId, processedBytes);
  }

  private boolean checkForWorkStealingBaseConditions(final List<String> scheduleGroup) {
    if (scheduleGroup.isEmpty()) {
      return false;
    }

    if (scheduleGroup.stream().anyMatch(stageId -> stageIdToWorkStealingExecuted.get(stageId).equals(true))) {
      return false;
    }
    LOG.error("[HWARIM]checking for work stealing conditions in {}...", scheduleGroup);
    final boolean executorStatus = executorRegistry.isExecutorSlotAvailable();
    final int totalNumberOfSlots = executorRegistry.getTotalNumberOfExecutorSlots();
    int remainingTasks = 0;
    for (String stage : scheduleGroup) {
      remainingTasks += planStateManager.getNumberOfTasksRemainingInStage(stage); // ready + executing?
    }
    return executorStatus && (totalNumberOfSlots > remainingTasks);
  }

  private Set<String> getCurrentlyRunningTaskId(final List<String> scheduleGroup) {
    final Set<String> onGoingTasksOfSchedulingGroup = new HashSet<>();
    for (String stageId : scheduleGroup) {
      onGoingTasksOfSchedulingGroup.addAll(planStateManager.getOngoingTaskIdsInStage(stageId));
    }
    return onGoingTasksOfSchedulingGroup;
  }

  private Map<String, Set<String>> getParentStages(final List<String> scheduleGroup) {
    Map<String, Set<String>> parentStages = new HashMap<>();
    for (String stageId : scheduleGroup) {
      parentStages.put(stageId, planStateManager.getPhysicalPlan().getStageDAG().getParents(stageId).stream()
        .map(Vertex::getId)
        .collect(Collectors.toSet()));
    }
    return parentStages;
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


  private void haltExecutors() {
    // driver sends message to executors
    // ask executors to flush metric
    LOG.error("haulting executors...");
    ControlMessage.Message haltExecutorsMessage = ControlMessage.Message.newBuilder()
      .setId(RuntimeIdManager.generateMessageId())
      .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
      .setType(ControlMessage.MessageType.HaltExecutors)
      .build();
    executorRegistry.viewExecutors(executors -> executors.forEach(executor ->
      executor.sendControlMessage(haltExecutorsMessage)));
    try {
      Thread.sleep(1000); // wait for 1 sec
    } catch (InterruptedException ignored) {
    }
  }

  private Map<String, Long> getCurrentlyTakenExecutionTimeMsOfRunningTasks(final List<String> scheduleGroup) {
    final Map<String, Long> taskToExecutionTime = new HashMap<>();
    for (String stageId : scheduleGroup) {
      taskToExecutionTime.putAll(planStateManager.getExecutingTaskToRunningTimeMs(stageId));
    }
    return taskToExecutionTime;
  }

  private List<String> getScheduleGroupByStage(final String stageId) {
    return sortedScheduleGroups.get(
      planStateManager.getPhysicalPlan().getStageDAG().getVertexById(stageId).getScheduleGroup())
      .stream()
      .map(Vertex::getId)
      .collect(Collectors.toList());
  }

  /**
   * Detect skewed tasks.
   * @param scheduleGroup current schedule group.
   * @return  skewed task information.
   *          key: taskId
   *          value:  left: current(paused) iterator index
   *                  right: total number of iterator
   */
  private Map<String, Pair<Integer, Integer>> detectSkew(final List<String> scheduleGroup) {
    final Map<String, Pair<Integer, Integer>> taskIdToIteratorInformation = new HashMap<>();
    final Map<String, Long> taskIdToInitializationOverhead = new HashMap<>();
    LOG.error("Skew detection in {}", scheduleGroup);
    // if this schedule group contains a source stage, return empty list
    if (scheduleGroup.stream().anyMatch(stage ->
      planStateManager.getPhysicalPlan().getStageDAG().getParents(stage).isEmpty())) {
      LOG.error("returning since this is a source stage...");
      return new HashMap<>();
    }

    // get size of running tasks
    workStealingCandidates.addAll(getCurrentlyRunningTaskId(scheduleGroup));
    final Map<String, Set<String>> parentStageId = getParentStages(scheduleGroup);
    final Map<String, Long> inputSizeOfCandidateTasks = new HashMap<>();
    for (String stage : scheduleGroup) {
      inputSizeOfCandidateTasks.putAll(
        getInputSizesOfCurrentlyRunningTaskIds(parentStageId.get(stage), workStealingCandidates));
    }
    LOG.error("currently running task id: {}", workStealingCandidates);
    LOG.error("parent stage ID: {}", parentStageId);
    LOG.error("input size: {}", inputSizeOfCandidateTasks);

    // get time taken to process the bytes
    Map<String, Long> taskIdToElapsedTime = getCurrentlyTakenExecutionTimeMsOfRunningTasks(scheduleGroup);


    // gather the arrived metric information
    boolean needToWait = true;
    long waitingStartTime = System.currentTimeMillis();
    while (needToWait) {
      needToWait = false;
      for (String taskId : workStealingCandidates) {
        TaskMetric taskMetric = metricStore.getMetricWithId(TaskMetric.class, taskId);
        if (taskMetric.getSerializedReadBytes() == -1) {
          needToWait = true;
          break;
        }
        taskIdToProcessedBytes.put(taskId, taskMetric.getSerializedReadBytes());
        taskIdToIteratorInformation.put(taskId, Pair.of(
          taskMetric.getCurrentIteratorIndex(), taskMetric.getTotalIteratorNumber()));
        taskIdToInitializationOverhead.put(taskId, taskMetric.getTaskPreparationTime());
      }
      if (System.currentTimeMillis() - waitingStartTime > 1000) {
        LOG.error("wait for 1 sec but information not gathered: quit");
        return new HashMap<>();
      }
    }

    haltExecutors();

    LOG.error("task id to processed bytes: {}", taskIdToProcessedBytes);
    LOG.error("task id to elapsed time: {}", taskIdToElapsedTime);

    // estimate the remaining time
    List<Pair<String, Long>> estimatedTimeToFinishPerTask = new ArrayList<>(taskIdToElapsedTime.size());
    for (String taskId : taskIdToProcessedBytes.keySet()) {
      // if processed bytes are not available, do not detect skew.
      if (taskIdToProcessedBytes.get(taskId) <= 0) {
        return new HashMap<>();
      }
      // if this task is almost finished, ignore it.
      Pair<Integer, Integer> iteratorInformation = taskIdToIteratorInformation.get(taskId);
      if (iteratorInformation.right() - iteratorInformation.left() <= 2) {
        continue;
      }
      long timeToFinishExecute = taskIdToElapsedTime.get(taskId) * inputSizeOfCandidateTasks.get(taskId)
        / taskIdToProcessedBytes.get(taskId);

      // if the estimated left time is shorter than the initialization overhead, stop!
      if (timeToFinishExecute < taskIdToInitializationOverhead.get(taskId) * 2) {
        continue;
      }
      LOG.error("{} estimated time to finish {}", taskId, timeToFinishExecute);
      estimatedTimeToFinishPerTask.add(Pair.of(taskId, timeToFinishExecute));
    }
    // detect skew
    Collections.sort(estimatedTimeToFinishPerTask, new Comparator<Pair<String, Long>>() {
      @Override
      public int compare(final Pair<String, Long> o1, final Pair<String, Long> o2) {
        return o2.right().compareTo(o1.right());
    }
    });

    final List<Pair<String, Long>> skewedTasks = estimatedTimeToFinishPerTask
      .subList(0, estimatedTimeToFinishPerTask.size() / 2);

    final Map<String, Pair<Integer, Integer>> skewedTasksWithIteratorInfo = new HashMap<>();
    for (Pair<String, Long> task : skewedTasks) {
      skewedTasksWithIteratorInfo.put(task.left(), taskIdToIteratorInformation.get(task.left()));
    }
    LOG.error("skewed tasks: {}", skewedTasks);
    LOG.error("skewed tasks with iterator info : {}", skewedTasksWithIteratorInfo);
    return skewedTasksWithIteratorInfo;
  }

  private void sendWorkStealingResultToExecutor(final Map<String, Pair<Integer, Integer>> result) {
    // driver sends message to executors
    // ask executors to flush metric
    final byte[] serialized = SerializationUtils.serialize((Serializable) result);
    ControlMessage.Message message = ControlMessage.Message.newBuilder()
      .setId(RuntimeIdManager.generateMessageId())
      .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
      .setType(ControlMessage.MessageType.SendWorkStealingResult)
      .setSendWorkStealingResult(ControlMessage.WorkStealingResultMsg.newBuilder()
        .setWorkStealingResult(ByteString.copyFrom(serialized))
        .build())
      .build();
    executorRegistry.viewExecutors(executors -> executors.forEach(executor -> executor.sendControlMessage(message)));
  }
}
