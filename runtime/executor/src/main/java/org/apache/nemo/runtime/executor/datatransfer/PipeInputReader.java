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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ShuffleExecutorSetProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.TaskIndexToExecutorIDProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.MetricMessageSender;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Represents the input data transfer to a task.
 */
public final class PipeInputReader implements InputReader {
  private final PipeManagerWorker pipeManagerWorker;
  private final MetricMessageSender metricMessageSender;

  private final int dstTaskIndex;

  /**
   * Attributes that specify how we should read the input.
   */
  private final IRVertex srcVertex;
  private final StageEdge runtimeEdge;

  PipeInputReader(final String dstTaskId,
                  final IRVertex srcIRVertex,
                  final StageEdge runtimeEdge,
                  final PipeManagerWorker pipeManagerWorker,
                  final MetricMessageSender metricMessageSender) {
    this.dstTaskIndex = RuntimeIdManager.getIndexFromTaskId(dstTaskId);
    this.srcVertex = srcIRVertex;
    this.runtimeEdge = runtimeEdge;
    this.pipeManagerWorker = pipeManagerWorker;
    this.metricMessageSender = metricMessageSender;
  }

  @Override
  public List<CompletableFuture<DataUtil.IteratorWithNumBytes>> read() {
    final Optional<CommunicationPatternProperty.Value> comValueOptional =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);
    final CommunicationPatternProperty.Value comValue = comValueOptional.orElseThrow(IllegalStateException::new);

    if (comValue.equals(CommunicationPatternProperty.Value.ONE_TO_ONE)) {
      return Collections.singletonList(pipeManagerWorker.read(dstTaskIndex, runtimeEdge, dstTaskIndex));
    } else if (comValue.equals(CommunicationPatternProperty.Value.BROADCAST)
      || comValue.equals(CommunicationPatternProperty.Value.SHUFFLE)) {
      final int numSrcTasks = InputReader.getSourceParallelism(this);
      final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
      for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
        futures.add(pipeManagerWorker.read(srcTaskIdx, runtimeEdge, dstTaskIndex));
      }
      return futures;
    } else if (comValue.equals(CommunicationPatternProperty.Value.PARTIAL_SHUFFLE)) {
      final int numSrcTasks = InputReader.getSourceParallelism(this);
      final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
      final List<String> dstTaskExecutorIDList = runtimeEdge.getDst().getExecutionProperties()
        .get(TaskIndexToExecutorIDProperty.class).get().get(dstTaskIndex);
      final String dstTaskExecutorID = dstTaskExecutorIDList.get(dstTaskExecutorIDList.size() - 1);
      for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
        final Integer srcTaskIndex = srcTaskIdx;
        final List<String> srcTaskExecutorIDList = runtimeEdge.getSrc().getExecutionProperties()
          .get(TaskIndexToExecutorIDProperty.class).get().get(srcTaskIndex);
        final String srcTaskExecutorID = srcTaskExecutorIDList.get(srcTaskExecutorIDList.size() - 1);
        // if destination hashset containing the destination task's node contains the source task's node.
        if (runtimeEdge.getDst().getExecutionProperties().get(ShuffleExecutorSetProperty.class).get().stream()
          .filter(p -> p.left().contains(dstTaskExecutorID))
          .anyMatch(p -> p.left().contains(srcTaskExecutorID))) {
          // add the future to the source and task.
          futures.add(pipeManagerWorker.read(srcTaskIdx, runtimeEdge, dstTaskIndex));
        }
      }
      return futures;
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  @Override
  public CompletableFuture<DataUtil.IteratorWithNumBytes> retry(final int index) {
    throw new UnsupportedOperationException(String.valueOf(index));
  }

  @Override
  public ExecutionPropertyMap<EdgeExecutionProperty> getProperties() {
    return runtimeEdge.getExecutionProperties();
  }

  @Override
  public IRVertex getSrcIrVertex() {
    return srcVertex;
  }
}
