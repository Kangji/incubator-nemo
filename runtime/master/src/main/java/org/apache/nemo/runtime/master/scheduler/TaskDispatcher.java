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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.nemo.common.ir.vertex.executionproperty.BarrierProperty;
import org.apache.nemo.compiler.optimizer.pass.runtime.IntermediateAccumulatorInsertionPass;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.PipeManagerMaster;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Dispatches tasks to executors in discrete batches (dispatch iterations).
 * A dispatch iteration occurs under one of the following conditions
 * - An executor slot becomes available (for reasons such as task completion/failure, or executor addition)
 * - A new list of tasks become available (for reasons such as stage completion, task failure, or executor removal)
 */
@DriverSide
@NotThreadSafe
final class TaskDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class.getName());
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
  private final ExecutorService dispatcherThread;
  private final PlanStateManager planStateManager;
  private boolean isSchedulerRunning;
  private boolean isTerminated;

  private final DelayedSignalingCondition schedulingIteration = new DelayedSignalingCondition();
  private final ExecutorRegistry executorRegistry;
  private final SchedulingConstraintRegistry schedulingConstraintRegistry;
  private final SchedulingPolicy schedulingPolicy;

  private final Scheduler scheduler;
  private final PipeManagerMaster pipeManagerMaster;
  private final PlanRewriter planRewriter;

  @Inject
  private TaskDispatcher(final Scheduler scheduler,
                         final SchedulingConstraintRegistry schedulingConstraintRegistry,
                         final SchedulingPolicy schedulingPolicy,
                         final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                         final ExecutorRegistry executorRegistry,
                         final PlanStateManager planStateManager,
                         final PipeManagerMaster pipeManagerMaster,
                         final PlanRewriter planRewriter) {
    this.scheduler = scheduler;
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.dispatcherThread = Executors.newSingleThreadExecutor(runnable ->
      new Thread(runnable, "TaskDispatcher thread"));
    this.planStateManager = planStateManager;
    this.isSchedulerRunning = false;
    this.isTerminated = false;
    this.executorRegistry = executorRegistry;
    this.schedulingPolicy = schedulingPolicy;
    this.schedulingConstraintRegistry = schedulingConstraintRegistry;
    this.pipeManagerMaster = pipeManagerMaster;
    this.planRewriter = planRewriter;
  }

  /**
   * Static constructor for manual usage.
   * @param schedulingConstraintRegistry Registry for the scheduling constraints.
   * @param schedulingPolicy The Scheduling Policy to use.
   * @param pendingTaskCollectionPointer A pointer to the pending tasks to be executed later on.
   * @param executorRegistry Registry for the list of executors available.
   * @param planStateManager Manager for the state of the plan being executed.
   * @param pipeManagerMaster Manager for the pipe data transfer for stream processing.
   * @param planRewriter     The Plan Rewriter for runtime optimization.
   * @return a new instance of task dispatcher.
   */
  public static TaskDispatcher newInstance(final Scheduler scheduler,
                                           final SchedulingConstraintRegistry schedulingConstraintRegistry,
                                           final SchedulingPolicy schedulingPolicy,
                                           final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                                           final ExecutorRegistry executorRegistry,
                                           final PlanStateManager planStateManager,
                                           final PipeManagerMaster pipeManagerMaster,
                                           final PlanRewriter planRewriter) {
    return new TaskDispatcher(scheduler, schedulingConstraintRegistry, schedulingPolicy, pendingTaskCollectionPointer,
      executorRegistry, planStateManager, pipeManagerMaster, planRewriter);
  }

  /**
   * A separate thread is run to dispatch tasks to executors.
   * See comments in the {@link Scheduler} for avoiding race conditions.
   */
  private final class TaskDispatcherThread implements Runnable {
    @Override
    public void run() {
      while (!isTerminated) {
        doScheduleTaskList();
        schedulingIteration.await();
      }

      if (planStateManager.isPlanDone()) {
        LOG.info("{} is complete.", planStateManager.getPlanId());
      } else {
        LOG.info("{} is incomplete.", planStateManager.getPlanId());
      }
      LOG.info("TaskDispatcher Terminated!");
    }
  }

  private void doScheduleTaskList() {
    final Optional<Collection<Task>> taskListOptional = pendingTaskCollectionPointer.getAndSetNull();
    if (!taskListOptional.isPresent()) {
      // Task list is empty
      LOG.debug("PendingTaskCollectionPointer is empty. Awaiting for more Tasks...");
      return;
    }

    final Collection<Task> taskList = taskListOptional.get();
    final List<Task> couldNotSchedule = new ArrayList<>();
    for (final Task task : taskList) {
      if (!planStateManager.getTaskState(task.getTaskId()).equals(TaskState.State.READY)) {
        // Guard against race conditions causing duplicate task launches
        LOG.warn("Skipping task {} as it is not READY", task.getTaskId());
        continue;
      }

      final Optional<String> barrierProperty = task.getPropertyValue(BarrierProperty.class);
      if (barrierProperty.isPresent()) {
        // Trigger the runtime pass
        final Set<StageEdge> targetEdges = BatchSchedulerUtils.getEdgesToOptimize(planStateManager, task.getTaskId());
        final int messageId = BatchSchedulerUtils.getMessageId(targetEdges);
        final List<ControlMessage.RunTimePassMessageEntry> data = new ArrayList<>();

        if (IntermediateAccumulatorInsertionPass.class.getCanonicalName().equals(barrierProperty.get())) {
          final HashSet<String> dataLocationExecutorNodeNames = task.getTaskIncomingEdges().stream()
            .map(e -> pipeManagerMaster.getLocation(e.getId(), RuntimeIdManager.getIndexFromTaskId(task.getTaskId())))
            .map(executorRegistry::executorIdToNodeName)
            .collect(Collectors.toCollection(HashSet::new));

          data.add(ControlMessage.RunTimePassMessageEntry.newBuilder()
            .setKey(IntermediateAccumulatorInsertionPass.EXECUTOR_SOURCE_KEY)
            .setValue(SerializationUtils.serialize(dataLocationExecutorNodeNames)).build());
        }

        planRewriter.accumulate(messageId, targetEdges, data);
        final PhysicalPlan newPhysicalPlan = planRewriter.rewrite(messageId);

        // Asynchronous call to the update of the physical plan on the scheduler.
        new Thread(() -> scheduler.updatePlan(newPhysicalPlan)).start();
        return;
      }

      executorRegistry.viewExecutors(executors -> {
        final MutableObject<Set<ExecutorRepresenter>> candidateExecutors = new MutableObject<>(executors);
        // Filter out the candidate executors that do not meet scheduling constraints.
        task.getExecutionProperties().forEachProperties(property -> {
          final Optional<SchedulingConstraint> constraint = schedulingConstraintRegistry.get(property.getClass());
          if (constraint.isPresent() && !candidateExecutors.getValue().isEmpty()) {
            candidateExecutors.setValue(candidateExecutors.getValue().stream()
              .filter(e -> constraint.get().testSchedulability(e, task))
              .collect(Collectors.toSet()));
          }
        });
        if (!candidateExecutors.getValue().isEmpty()) {
          // Select executor
          final ExecutorRepresenter selectedExecutor
            = schedulingPolicy.selectExecutor(candidateExecutors.getValue(), task);
          // update metadata first
          planStateManager.onTaskStateChanged(task.getTaskId(), TaskState.State.EXECUTING);

          LOG.info("{} scheduled to {}", task.getTaskId(), selectedExecutor.getExecutorId());
          // send the task
          selectedExecutor.onTaskScheduled(task);
        } else {
          couldNotSchedule.add(task);
        }
      });
    }

    LOG.debug("All except {} were scheduled among {}", new Object[]{couldNotSchedule, taskList});
    if (couldNotSchedule.size() > 0) {
      // Try these again, if no new task list has been set
      pendingTaskCollectionPointer.setIfNull(couldNotSchedule);
    }
  }

  /**
   * Signals to the condition on executor slot availability.
   */
  void onExecutorSlotAvailable() {
    schedulingIteration.signal();
  }

  /**
   * Signals to the condition on the Task collection availability.
   */
  void onNewPendingTaskCollectionAvailable() {
    schedulingIteration.signal();
  }

  /**
   * Run the dispatcher thread.
   */
  void run() {
    if (!isTerminated && !isSchedulerRunning) {
      dispatcherThread.execute(new TaskDispatcherThread());
      dispatcherThread.shutdown();
      isSchedulerRunning = true;
    }
  }

  void terminate() {
    isTerminated = true;
    schedulingIteration.signal();
  }

  /**
   * A {@link Condition} that allows 'delayed' signaling.
   */
  private final class DelayedSignalingCondition {
    private boolean hasDelayedSignal = false;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    /**
     * Signals to this condition. If no thread is awaiting for this condition,
     * signaling is delayed until the first next {@link #await} invocation.
     */
    void signal() {
      lock.lock();
      try {
        hasDelayedSignal = true;
        condition.signal();
      } finally {
        lock.unlock();
      }
    }

    /**
     * Awaits to this condition. The thread will awake when there is a delayed signal,
     * or the next first {@link #signal} invocation.
     */
    void await() {
      lock.lock();
      try {
        while (!hasDelayedSignal) { // to handle spurious wakeups
          condition.await();
        }
        hasDelayedSignal = false;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } finally {
        lock.unlock();
      }
    }
  }
}
