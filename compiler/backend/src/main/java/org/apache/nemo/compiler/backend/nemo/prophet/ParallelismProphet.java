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

package org.apache.nemo.compiler.backend.nemo.prophet;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.SimulationException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.metric.TaskMetric;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PhysicalPlanGenerator;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.scheduler.SimulationScheduler;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A prophet for Parallelism.
 */
public final class ParallelismProphet implements Prophet {
  private final SimulationScheduler simulationScheduler;
  private final PhysicalPlanGenerator physicalPlanGenerator;
  private final IRDAG currentIRDAG;
  private final PhysicalPlan currentPhysicalPlan;
  private final Set<IREdge> edgesToOptimize;
  private int partitionerProperty;

  public ParallelismProphet(final IRDAG irdag, final PhysicalPlan physicalPlan,
                            final PhysicalPlanGenerator physicalPlanGenerator,
                            final Set<IREdge> edgesToOptimize) {
    this.currentIRDAG = irdag;
    this.currentPhysicalPlan = physicalPlan;
    this.physicalPlanGenerator = physicalPlanGenerator;
    this.edgesToOptimize = edgesToOptimize;
    calculatePartitionerProperty(edgesToOptimize);
    try {
      this.simulationScheduler = Tang.Factory.getTang().newInjector().getInstance(SimulationScheduler.class);
    } catch (final InjectionException e) {
      throw new SimulationException(e);
    }
  }

  private Pair<Integer, Long> launchSimulationForPlan(final PhysicalPlan physicalPlan) {
    this.simulationScheduler.schedulePlan(physicalPlan, 1);
    final MetricStore resultingMetricStore = this.simulationScheduler.collectMetricStore();
    final List<Pair<Integer, Long>> taskSizeRatioToDuration = new ArrayList<>();
    resultingMetricStore.getMetricMap(TaskMetric.class).values().forEach(taskMetric -> {
      taskSizeRatioToDuration.add(Pair.of(((TaskMetric) taskMetric).getTaskSizeRatio(),
        ((TaskMetric) taskMetric).getTaskDuration()));
    });
    return Collections.min(taskSizeRatioToDuration, Comparator.comparing(Pair::right));
  }

  @Override
  public Map<String, Long> calculate() {
    final Map<String, Long> result = new HashMap<>();
    final List<PhysicalPlan> listOfPhysicalPlans = new ArrayList<>(); // when to update here?
    for (int i = 0; i < 7; i++) {
      final int parallelism = (int) (partitionerProperty / Math.pow(2, i));
      PhysicalPlan newPlan = makePhysicalPlanForSimulation(parallelism, edgesToOptimize, currentIRDAG);
      listOfPhysicalPlans.add(newPlan);
    }
    // is this right?
    final Pair<Integer, Long> pairWithMinDuration =
      listOfPhysicalPlans.stream().map(this::launchSimulationForPlan).min(Comparator.comparing(p -> p.right())).get();

    this.simulationScheduler.terminate();

    result.put("opt.parallelism", pairWithMinDuration.left().longValue());
    return result;
  }

  private void setPartitionerProperty(final int partitionerProperty) {
    this.partitionerProperty = partitionerProperty;
  }
  private void calculatePartitionerProperty(final Set<IREdge> edges) {
    setPartitionerProperty(edges.iterator().next().getPropertyValue(PartitionerProperty.class).get().right());
  }

  private PhysicalPlan makePhysicalPlanForSimulation(final int parallelism,
                                                     final Set<IREdge> edges,
                                                     final IRDAG currentDag) {
    Set<IRVertex> verticesToChangeParallelism = edges.stream()
      .map(edge -> edge.getDst()).collect(Collectors.toSet());
    final IRDAG newDag = currentDag;
    newDag.topologicalDo(v -> {
      if (verticesToChangeParallelism.contains(v)) {
        v.setProperty(ParallelismProperty.of(parallelism));
      }
    });
    final DAG<Stage, StageEdge> stageDag = physicalPlanGenerator.apply(newDag);
    return new PhysicalPlan(currentPhysicalPlan.getPlanId().concat("-" + parallelism), stageDag);
  }
}
