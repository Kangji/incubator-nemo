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
package edu.snu.onyx.runtime.executor;

import edu.snu.onyx.common.ContextImpl;
import edu.snu.onyx.common.exception.BlockFetchException;
import edu.snu.onyx.common.exception.BlockWriteException;
import edu.snu.onyx.common.ir.Reader;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import edu.snu.onyx.runtime.common.plan.RuntimeEdge;
import edu.snu.onyx.runtime.common.plan.physical.*;
import edu.snu.onyx.runtime.common.state.TaskGroupState;
import edu.snu.onyx.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.onyx.runtime.executor.datatransfer.InputReader;
import edu.snu.onyx.runtime.executor.datatransfer.OutputCollectorImpl;
import edu.snu.onyx.runtime.executor.datatransfer.OutputWriter;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Executes a task group.
 */
public final class TaskGroupExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(TaskGroupExecutor.class.getName());

  private final TaskGroup taskGroup;
  private final TaskGroupStateManager taskGroupStateManager;
  private final List<PhysicalStageEdge> stageIncomingEdges;
  private final List<PhysicalStageEdge> stageOutgoingEdges;
  private final DataTransferFactory channelFactory;

  /**
   * Map of task IDs in this task group to their readers/writers.
   */
  private final Map<String, List<InputReader>> taskIdToInputReaderMap;
  private final Map<String, List<OutputWriter>> taskIdToOutputWriterMap;

  private final Map<String, OutputCollectorImpl> taskIdToOutputPipeMap;
  private final Map<String, List<OutputCollectorImpl>> taskIdToInputPipeMap;
  private final AtomicInteger sourceParallelism;
  private final List<String> taskList;
  private final BlockingQueue<Object> interTaskGroupData;
  private boolean isExecutionRequested;

  public TaskGroupExecutor(final TaskGroup taskGroup,
                           final TaskGroupStateManager taskGroupStateManager,
                           final List<PhysicalStageEdge> stageIncomingEdges,
                           final List<PhysicalStageEdge> stageOutgoingEdges,
                           final DataTransferFactory channelFactory) {
    this.taskGroup = taskGroup;
    this.taskGroupStateManager = taskGroupStateManager;
    this.stageIncomingEdges = stageIncomingEdges;
    this.stageOutgoingEdges = stageOutgoingEdges;
    this.channelFactory = channelFactory;

    this.taskIdToInputReaderMap = new HashMap<>();
    this.taskIdToOutputWriterMap = new HashMap<>();
    this.taskIdToInputPipeMap = new HashMap<>();
    this.taskIdToOutputPipeMap = new HashMap<>();
    this.sourceParallelism = new AtomicInteger(0);
    this.taskList = new ArrayList<>();
    this.interTaskGroupData = new LinkedBlockingQueue<>();

    this.isExecutionRequested = false;

    initializeDataTransfer();
  }

  /**
   * Initializes readers and writers depending on the execution properties.
   * Note that there are edges that are cross-stage and stage-internal.
   */
  private void initializeDataTransfer() {
    taskGroup.getTaskDAG().topologicalDo((task -> {
      final Set<PhysicalStageEdge> inEdgesFromOtherStages = getInEdgesFromOtherStages(task);
      final Set<PhysicalStageEdge> outEdgesToOtherStages = getOutEdgesToOtherStages(task);

      // Add InputReaders for inter-stage data transfer
      inEdgesFromOtherStages.forEach(physicalStageEdge -> {
        final InputReader inputReader = channelFactory.createReader(
            task, physicalStageEdge.getSrcVertex(), physicalStageEdge);
        addInputReader(task, inputReader);
        System.out.println(String.format("log: Added InputReader, %s %s %s\n", taskGroup.getTaskGroupId(),
            task.getId(), task.getRuntimeVertexId()));
        sourceParallelism.getAndAdd(physicalStageEdge.getSrcVertex().getProperty(ExecutionProperty.Key.Parallelism));
      });

      // Add OutputWriters for inter-stage data transfer
      outEdgesToOtherStages.forEach(physicalStageEdge -> {
        System.out.println(String.format("log: Added OutputWriter, %s %s %s", taskGroup.getTaskGroupId(),
            task.getId(), task.getRuntimeVertexId()));

        final OutputWriter outputWriter = channelFactory.createWriter(
            task, physicalStageEdge.getDstVertex(), physicalStageEdge);
        addOutputWriter(task, outputWriter);
      });

      // Add OutputCollectors
      addOutputPipe(task);

      // Add InputPipes for intra-stage data transfer
      if (needsInputPipe(task)) {
        addInputPipe(task);
      }

      taskList.add(task.getId());
    }));

    System.out.println(String.format("log: pendingTasks: %d", taskList.size()));

    if (sourceParallelism.get() == 0) {
      sourceParallelism.getAndAdd(1);
    }
  }

  // Helper functions to initializes cross-stage edges.
  private Set<PhysicalStageEdge> getInEdgesFromOtherStages(final Task task) {
    return stageIncomingEdges.stream().filter(
        stageInEdge -> stageInEdge.getDstVertex().getId().equals(task.getRuntimeVertexId()))
        .collect(Collectors.toSet());
  }

  private Set<PhysicalStageEdge> getOutEdgesToOtherStages(final Task task) {
    return stageOutgoingEdges.stream().filter(
        stageInEdge -> stageInEdge.getSrcVertex().getId().equals(task.getRuntimeVertexId()))
        .collect(Collectors.toSet());
  }

  // Helper functions to add the initialized reader/writer to the maintained map.
  private void addInputReader(final Task task, final InputReader inputReader) {
    taskIdToInputReaderMap.computeIfAbsent(task.getId(), readerList -> new ArrayList<>());
    taskIdToInputReaderMap.get(task.getId()).add(inputReader);
  }

  private void addOutputWriter(final Task task, final OutputWriter outputWriter) {
    taskIdToOutputWriterMap.computeIfAbsent(task.getId(), readerList -> new ArrayList<>());
    taskIdToOutputWriterMap.get(task.getId()).add(outputWriter);
  }

  private boolean needsInputPipe(final Task task) {
    return !taskGroup.getTaskDAG().getIncomingEdgesOf(task).isEmpty() && !hasInputReader(task);
  }

  private void closeOutputWriters() {
    for (final Map.Entry<String, List<OutputWriter>> entry : taskIdToOutputWriterMap.entrySet()) {
      final String taskId = entry.getKey();
      final List<OutputWriter> outputWriters = entry.getValue();
      outputWriters.forEach(outputWriter -> {
        outputWriter.close();
        System.out.println(String.format("log: %s %s Closed OutputWriter(commited block!) %s",
            taskGroup.getTaskGroupId(), taskId, outputWriter.getId()));
      });
    }
  }

  private void addInputPipe(final Task task) {
    List<OutputCollectorImpl> localPipes = new ArrayList<>();
    List<Task> parentTasks = taskGroup.getTaskDAG().getParents(task.getId());

    if (parentTasks != null) {

      System.out.println(String.format("log: Added InputPipe, %s %s %s", taskGroup.getTaskGroupId(),
          task.getId(), task.getRuntimeVertexId()));

      parentTasks.forEach(parent -> {
        System.out.println(String.format("log: Parents of %s %s: %s",
            taskGroup.getTaskGroupId(), task.getRuntimeVertexId(), parent.getRuntimeVertexId()));
        localPipes.add(taskIdToOutputPipeMap.get(parent.getId()));
      });

      taskIdToInputPipeMap.put(task.getId(), localPipes);
    } else {
      taskIdToInputPipeMap.put(task.getId(), null);
    }
  }

  private void addOutputPipe(final Task task) {
    taskIdToOutputPipeMap.put(task.getId(), new OutputCollectorImpl());
  }

  private boolean hasInputReader(final Task task) {
    return taskIdToInputReaderMap.containsKey(task.getId());
  }

  private boolean hasOutputWriter(final Task task) {
    return taskIdToOutputWriterMap.containsKey(task.getId());
  }

  private boolean allInputPipesEmpty() {
    AtomicInteger nonEmptyInputPipe = new AtomicInteger(0);
    taskIdToInputPipeMap.entrySet().stream().forEach(entry -> {
      final List<OutputCollectorImpl> localReaders = entry.getValue();
      localReaders.forEach(localReader -> {
        if (!localReader.isEmpty()) {
          nonEmptyInputPipe.getAndIncrement();
        }
      });
    });

    return nonEmptyInputPipe.get() == 0;
  }

  // Check whether this task has no intra-TaskGroup input left to process.
  private boolean allInputPipesEmpty(final Task task) {
    if (!hasInputReader(task)) {
      AtomicInteger nonEmptyInputPipes = new AtomicInteger(0);
      taskIdToInputPipeMap.get(task.getId()).forEach(localReader -> {
        if (!localReader.isEmpty()) {
          nonEmptyInputPipes.getAndIncrement();
        }
      });
      return nonEmptyInputPipes.get() == 0;
    } else {
      return true;
    }
  }

  /**
   * Executes the task group.
   */
  public void execute() {
    System.out.println(String.format("%s Execution Started!", taskGroup.getTaskGroupId()));

    if (isExecutionRequested) {
      throw new RuntimeException("TaskGroup {" + taskGroup.getTaskGroupId() + "} execution called again!");
    } else {
      isExecutionRequested = true;
    }

    taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.EXECUTING, Optional.empty(), Optional.empty());

    while (!isTaskGroupComplete()) {
      taskGroup.getTaskDAG().topologicalDo(task -> {
        try {
          if (taskList.contains(task.getId()) && !bypassTask(task)) {
            if (task instanceof BoundedSourceTask) {
              launchBoundedSourceTask((BoundedSourceTask) task);
            } else if (task instanceof OperatorTask) {
              launchOperatorTask((OperatorTask) task);
              checkTaskCompletion((OperatorTask) task);
            } else if (task instanceof MetricCollectionBarrierTask) {
              launchMetricCollectionBarrierTask((MetricCollectionBarrierTask) task);
            } else {
              throw new UnsupportedOperationException(task.toString());
            }
          }
        } catch (final BlockFetchException ex) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE,
              Optional.empty(), Optional.of(TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE));
          System.out.println(String.format("%s Execution Failed (Recoverable: input read failure)! Exception: %s",
              taskGroup.getTaskGroupId(), ex.toString()));
        } catch (final BlockWriteException ex2) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE,
              Optional.empty(), Optional.of(TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE));
          System.out.println(String.format("%s Execution Failed (Recoverable: output write failure)! Exception: %s",
              taskGroup.getTaskGroupId(), ex2.toString()));
        } catch (final Exception e) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_UNRECOVERABLE,
              Optional.empty(), Optional.empty());
          System.out.println(String.format("%s Execution Failed! Exception: %s",
              taskGroup.getTaskGroupId(), e.toString()));
          throw new RuntimeException(e);
        }
      });
    }

    closeOutputWriters();

    taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.COMPLETE, Optional.empty(), Optional.empty());
    System.out.println(String.format("%s Complete!", taskGroup.getTaskGroupId()));
  }

  private boolean bypassTask(final Task task) {
    // If inter-TG data aren't all consumed but local readers are empty,
    // the previous task is a per-key/window-aggregation task
    // and currently we need to bypass the following tasks until all the elements are aggregated.
    boolean bypassThisTask = false;

    if (!hasInputReader(task)) {
      bypassThisTask = !interTaskGroupData.isEmpty() && allInputPipesEmpty(task);
    }

    return bypassThisTask;
  }

  private boolean allParentTasksComplete(final Task task) {
    // If there is a parent task that hasn't yet completed,
    // then this task isn't complete.
    List<Task> parentTasks = taskGroup.getTaskDAG().getParents(task.getId());
    AtomicInteger parentTasksNotYetComplete = new AtomicInteger(0);
    parentTasks.forEach(parentTask -> {
      if (taskList.contains(parentTask.getId())) {
        parentTasksNotYetComplete.getAndIncrement();
      }
    });

    return parentTasksNotYetComplete.get() == 0;
  }

  // A Task can be marked as 'Complete' when the following two conditions are both met:
  // - All of its InputReader/LocalPipes are empty
  // - All of its parent Tasks are complete
  private void checkTaskCompletion(final OperatorTask task) {
    if ((hasInputReader(task) && interTaskGroupData.isEmpty()
        || !hasInputReader(task) && allInputPipesEmpty(task))
        && allParentTasksComplete(task)){

      if (taskList.contains(task.getId())) {
        taskList.remove(task.getId());
        System.out.println(String.format("log: %s %s Complete!", taskGroup.getTaskGroupId(),
            task.getId()));
        System.out.println(String.format("log: pendingTasks: %s", taskList));
      }
    }
  }

  private boolean isTaskGroupComplete() {
    return taskList.isEmpty();
  }

  /**
   * Processes a BoundedSourceTask.
   * @param sourceTask to execute
   * @throws Exception occurred during input read.
   */
  private void launchBoundedSourceTask(final BoundedSourceTask sourceTask) throws Exception {
    final Reader reader = sourceTask.getReader();
    final Iterator readData = reader.read();
    final List iterable = new ArrayList<>();
    readData.forEachRemaining(iterable::add);

    // Writes inter-TaskGroup data
    if (hasOutputWriter(sourceTask)) {
      taskIdToOutputWriterMap.get(sourceTask.getId()).forEach(outputWriter -> {
        iterable.forEach(data -> {
          System.out.println(String.format("log: %s %s %s From InputReader read %s", taskGroup.getTaskGroupId(),
              sourceTask.getId(), sourceTask.getRuntimeVertexId(), data));

          List<Object> iterable1 = Collections.singletonList(data);
          outputWriter.write(iterable1);
        });
      });
    } else {
      // Writes intra-TaskGroup data
      OutputCollectorImpl outputPipe = taskIdToOutputPipeMap.get(sourceTask.getId());
      try {
        iterable.forEach(data -> {
          outputPipe.emit(data);
          System.out.println(String.format("log: %s %s %s From OutputPipe wrote %s", taskGroup.getTaskGroupId(),
              sourceTask.getId(), sourceTask.getRuntimeVertexId(), data));
        });
      } catch (final Exception e) {
        throw new RuntimeException();
      }
    }

    taskList.remove(sourceTask.getId());
    System.out.println(String.format("log: %s %s Complete!", taskGroup.getTaskGroupId(), sourceTask.getId()));
  }

  /**
   * Processes an OperatorTask.
   * @param operatorTask to execute
   */
  private void launchOperatorTask(final OperatorTask operatorTask) {
    final Map<Transform, Object> sideInputMap = new HashMap<>();

    // Check for side inputs
    if (hasInputReader(operatorTask)) {
      taskIdToInputReaderMap.get(operatorTask.getId()).stream().filter(InputReader::isSideInputReader)
          .forEach(sideInputReader -> {
            try {
              final Object sideInput = sideInputReader.getSideInput().get();
              final RuntimeEdge inEdge = sideInputReader.getRuntimeEdge();
              final Transform srcTransform;
              if (inEdge instanceof PhysicalStageEdge) {
                srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex())
                    .getTransform();
              } else {
                srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
              }
              sideInputMap.put(srcTransform, sideInput);
            } catch (final InterruptedException | ExecutionException e) {
              throw new BlockFetchException(e);
            }
          });
    }

    final Transform.Context transformContext = new ContextImpl(sideInputMap);
    final Transform transform = operatorTask.getTransform();
    final OutputCollectorImpl outputCollector = taskIdToOutputPipeMap.get(operatorTask.getId());
    transform.prepare(transformContext, outputCollector);

    // Check for non-side inputs.
    if (hasInputReader(operatorTask)) {
      // Reads inter-TaskGroup data.
      if (interTaskGroupData.isEmpty()) {
        taskIdToInputReaderMap.get(operatorTask.getId()).stream()
            .filter(inputReader -> !inputReader.isSideInputReader()).forEach(inputReader -> {
          List<CompletableFuture<Iterator>> futures = inputReader.read();
          futures.forEach(compFuture -> {
            try {
              Iterator iterator = compFuture.get();
              iterator.forEachRemaining(data -> {
                System.out.println(String.format("log: %s %s Read from InputReader(Inter-TG): %s",
                    taskGroup.getTaskGroupId(), operatorTask.getId(), data));
                interTaskGroupData.add(data);
              });
            } catch (InterruptedException e) {
              throw new RuntimeException("Interrupted while waiting for InputReader.readElement()", e);
            } catch (ExecutionException e1) {
              throw new RuntimeException("ExecutionException while waiting for InputReader.readElement()", e1);
            }
          });
        });
      }
    }

    // Consumes the received element from incoming edges.
    IntStream.range(0, sourceParallelism.get()).forEach(srcTaskNum -> {
      try {
        Object data = null;
        if (hasInputReader(operatorTask)) {
          data = interTaskGroupData.take();
        } else {
          // Reads intra-TaskGroup data.
          for (OutputCollectorImpl localReader : taskIdToInputPipeMap.get(operatorTask.getId())) {
            if (!localReader.isEmpty()) {
              data = localReader.remove();
              System.out.println(String.format("log: %s %s: Reading from InputPipe(Intra-TG) %s",
                taskGroup.getTaskGroupId(), operatorTask.getId(), data.toString()));
            }
          }
        }

        // Because the data queue is a blocking queue, we may need to wait some available data to be pushed.
        transform.onData(data);
      } catch (final InterruptedException e) {
        throw new BlockFetchException(e);
      }
    });

    // TODO: For GroupByKeyTransform, trigger closing of transform per window.
    if (!hasInputReader(operatorTask)) {
      if (allInputPipesEmpty(operatorTask)) {
        transform.close(true);
        // Check whether there is any output data from the transform and write the output of this task to the writer.
        writeToOutputWriter(outputCollector, operatorTask);
      } else {
        transform.close(false);
      }
    } else {
      if (interTaskGroupData.isEmpty()) {
        transform.close(true);
        // Check whether there is any output data from the transform and write the output of this task to the writer.
        writeToOutputWriter(outputCollector, operatorTask);
      } else {
        transform.close(false);
      }
    }
  }

  private void writeToOutputWriter(final OutputCollectorImpl localWriter, final Task operatorTask) {
    if (hasOutputWriter(operatorTask)) {
      final List output = localWriter.collectOutputList();
      taskIdToOutputWriterMap.get(operatorTask.getId()).forEach(outputWriter -> {
        if (!output.isEmpty()) {

          output.forEach(data -> {
            System.out.println(String.format("log: %s %s: output from transform(after close): %s",
                taskGroup.getTaskGroupId(),
                operatorTask.getId(), data.toString()));

            List<Object> iterable = Collections.singletonList(data);
            outputWriter.write(iterable);
          });
        }
      });
    }
  }

  /**
   * Pass on the data to the following tasks.
   * @param task the task to carry on the data.
   */
  private void launchMetricCollectionBarrierTask(final MetricCollectionBarrierTask task) {
    final BlockingQueue<Iterable> dataQueue = new LinkedBlockingQueue<>();
    taskIdToInputReaderMap.get(task.getId()).stream().filter(inputReader -> !inputReader.isSideInputReader())
        .forEach(inputReader -> {
          inputReader.read().forEach(compFuture -> compFuture.thenAccept(dataQueue::add));
        });

    final List data = new ArrayList<>();
    IntStream.range(0, sourceParallelism.get()).forEach(srcTaskNum -> {
      try {
        final Iterable availableData = dataQueue.take();
        availableData.forEach(data::add);
      } catch (final InterruptedException e) {
        throw new BlockFetchException(e);
      }
    });
    taskIdToOutputWriterMap.get(task.getId()).forEach(outputWriter -> {
      outputWriter.write(data);
    });
  }
}
