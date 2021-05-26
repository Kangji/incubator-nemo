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
package org.apache.nemo.compiler.backend.nemo;

import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.MessageIdEdgeProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.utility.runtimepass.MessageAggregatorVertex;
import org.apache.nemo.compiler.backend.nemo.prophet.ParallelismProphet;
import org.apache.nemo.compiler.backend.nemo.prophet.Prophet;
import org.apache.nemo.compiler.backend.nemo.prophet.SkewProphet;
import org.apache.nemo.compiler.optimizer.NemoOptimizer;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.master.scheduler.SimulationScheduler;
import org.apache.reef.tang.InjectionFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Rewrites the physical plan during execution, to enforce the optimizations of Nemo RunTimePasses.
 * <p>
 * A high-level flow of a rewrite is as follows:
 * Runtime - (PhysicalPlan-level info) - NemoPlanRewriter - (IRDAG-level info) - NemoOptimizer - (new IRDAG)
 * - NemoPlanRewriter - (new PhysicalPlan) - Runtime
 * <p>
 * Here, the NemoPlanRewriter acts as a translator between the Runtime that only understands PhysicalPlan-level info,
 * and the NemoOptimizer that only understands IRDAG-level info.
 * <p>
 * This decoupling between the NemoOptimizer and the Runtime lets Nemo optimization policies dynamically control
 * distributed execution behaviors, and at the same time enjoy correctness/reusability/composability properties that
 * the IRDAG abstraction provides.
 *
 * @param <T> type of the data to aggregate.
 */
public final class NemoPlanRewriter<T> implements PlanRewriter {
  private static final Logger LOG = LoggerFactory.getLogger(NemoPlanRewriter.class.getName());
  private static final String DTS_KEY = "DTS";
  private final NemoOptimizer nemoOptimizer;
  private final NemoBackend nemoBackend;
  private final Map<Integer, Map<Object, T>> messageIdToAggregatedData;
  private CountDownLatch readyToRewriteLatch;
  private final InjectionFuture<SimulationScheduler> simulationSchedulerInjectionFuture;
  private final PhysicalPlanGenerator physicalPlanGenerator;

  private IRDAG currentIRDAG;
  private PhysicalPlan currentPhysicalPlan;

  @Inject
  public NemoPlanRewriter(final NemoOptimizer nemoOptimizer,
                          final NemoBackend nemoBackend,
                          final InjectionFuture<SimulationScheduler> simulationSchedulerInjectionFuture,
                          final PhysicalPlanGenerator physicalPlanGenerator) {
    this.nemoOptimizer = nemoOptimizer;
    this.nemoBackend = nemoBackend;
    this.simulationSchedulerInjectionFuture = simulationSchedulerInjectionFuture;
    this.physicalPlanGenerator = physicalPlanGenerator;
    this.messageIdToAggregatedData = new HashMap<>();
    this.readyToRewriteLatch = new CountDownLatch(1);
  }

  public void setCurrentIRDAG(final IRDAG currentIRDAG) {
    this.currentIRDAG = currentIRDAG;
  }

  public void setCurrentPhysicalPlan(final PhysicalPlan currentPhysicalPlan) {
    this.currentPhysicalPlan = currentPhysicalPlan;
  }
  @Override
  public PhysicalPlan rewrite(final int messageId) {
    try {
      this.readyToRewriteLatch.await();
    } catch (final InterruptedException e) {
      LOG.error("Interrupted while waiting for the rewrite latch: {}", e);
      Thread.currentThread().interrupt();
    }
    if (currentIRDAG == null) {
      throw new IllegalStateException();
    }
    final Map<Object, T> aggregatedData = messageIdToAggregatedData.remove(messageId); // remove for GC
    if (aggregatedData == null) {
      throw new IllegalStateException();
    }

    // Find IREdges using the messageId
    final Set<IREdge> examiningEdges = currentIRDAG
      .getVertices()
      .stream()
      .flatMap(v -> currentIRDAG.getIncomingEdgesOf(v).stream())
      .filter(e -> e.getPropertyValue(MessageIdEdgeProperty.class).isPresent()
        && e.getPropertyValue(MessageIdEdgeProperty.class).get().contains(messageId)
        && !(e.getDst() instanceof MessageAggregatorVertex))
      .collect(Collectors.toSet());
    if (examiningEdges.isEmpty()) {
      throw new IllegalArgumentException(String.valueOf(messageId));
    }

    // Optimize using the Message
    final Message<Map<Object, T>> message = new Message<>(messageId, examiningEdges, aggregatedData);
    final IRDAG newIRDAG = nemoOptimizer.optimizeAtRunTime(currentIRDAG, message);
    this.setCurrentIRDAG(newIRDAG);

    // Re-compile the IRDAG into a physical plan
    final PhysicalPlan newPhysicalPlan = nemoBackend.compile(newIRDAG);

    // Update the physical plan and return
    final List<Stage> currentStages = currentPhysicalPlan.getStageDAG().getTopologicalSort();
    final List<Stage> newStages = newPhysicalPlan.getStageDAG().getTopologicalSort();

    final DAGBuilder<Stage, StageEdge> stageBuilder = new DAGBuilder<>();
    boolean updatePlan = false;
    final Map<Stage, Stage> newToOldStageMap = new HashMap<>();
    for (int i = 0; i < newStages.size(); i++) {
      final List<IRVertex> currentStageInternalIRDAG = currentStages.get(i).getInternalIRDAG().getVertices();
      final List<IRVertex> newStageInternalIRDAG = newStages.get(i).getInternalIRDAG().getVertices();
      final int currentStageInternalIRDAGSize = currentStageInternalIRDAG.size();
      final int newStageInternalIRDAGSize = newStageInternalIRDAG.size();
      if (!updatePlan && currentStageInternalIRDAGSize == newStageInternalIRDAGSize
        && IntStream.range(0, currentStageInternalIRDAGSize).allMatch(
          j -> currentStageInternalIRDAG.get(j).getId().equals(newStageInternalIRDAG.get(j).getId()))) {
        final Stage stage = currentStages.get(i);
        newToOldStageMap.put(newStages.get(i), stage);

        stageBuilder.addVertex(stage);
        currentPhysicalPlan.getStageDAG().getIncomingEdgesOf(stage).forEach(stageBuilder::connectVertices);
        final ExecutionPropertyMap<VertexExecutionProperty> newProperties = newStages.get(i).getExecutionProperties();
        stage.setExecutionProperties(newProperties);
        newProperties.get(ParallelismProperty.class).ifPresent(newParallelism -> {
          stage.getTaskIndices().clear();
          stage.getTaskIndices().addAll(IntStream.range(0, newParallelism).boxed()
            .collect(Collectors.toList()));
          IntStream.range(stage.getVertexIdToReadables().size(), newParallelism).forEach(newIdx ->
            stage.getVertexIdToReadables().add(new HashMap<>()));
        });
      } else {
        updatePlan = true;
        final Stage newStage = newStages.get(i);
        stageBuilder.addVertex(newStage);
        newPhysicalPlan.getStageDAG().getIncomingEdgesOf(newStage).forEach(e -> {
          if (newToOldStageMap.containsKey(e.getSrc())) {
            stageBuilder.connectVertices(new StageEdge(e.getId(), e.getExecutionProperties(),
              e.getSrcIRVertex(), e.getDstIRVertex(),
              newToOldStageMap.get(e.getSrc()), e.getDst()));
          } else {
            stageBuilder.connectVertices(e);
          }
        });
      }
    }
    return new PhysicalPlan(currentPhysicalPlan.getPlanId(), stageBuilder.build());
  }

  /**
   * Accumulate the data needed in Plan Rewrite.
   * DATA_NOT_AUGMENTED indicates that the information need in rewrite is not stored in RunTimePassMessageEntry,
   * and we should explicitly generate it using Prophet class. In this case, the data will contain only one entry with
   * key as DATA_NOT_AUGMENTED.
   *
   * @param messageId     of the rewrite.
   * @param targetEdges   edges to change during rewrite.
   * @param data          to accumulate.
   */
  @Override
  public void accumulate(final int messageId, final Set<StageEdge> targetEdges,
                         final List<ControlMessage.RunTimePassMessageEntry> data) {
    final Map<String, T> aggregatedData;
    if (!data.isEmpty() && data.get(0).getKey().equals(DTS_KEY)) {
      final Prophet<String, Long> prophet =
        new ParallelismProphet(currentIRDAG, currentPhysicalPlan, simulationSchedulerInjectionFuture.get(),
          physicalPlanGenerator, targetEdges);
      aggregatedData = (Map<String, T>) prophet.calculate();
    } else if (SerializationUtils.deserialize(Base64.getDecoder().decode(data.get(0).getValue())) instanceof Long) {
      final Prophet<String, Long> prophet = new SkewProphet(data);
      aggregatedData = (Map<String, T>) prophet.calculate();
    } else {
      aggregatedData = new HashMap<>();
      data.forEach(e ->
        aggregatedData.put(e.getKey(), (T) SerializationUtils.deserialize(Base64.getDecoder().decode(e.getValue()))));
    }
    this.messageIdToAggregatedData.computeIfAbsent(messageId, id -> new HashMap<>()).putAll(aggregatedData);
    this.readyToRewriteLatch.countDown();
  }
}
