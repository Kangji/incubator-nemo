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
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.scheduler.SimulationScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A prophet for Parallelism.
 */
public final class ParallelismProphet implements Prophet {
  private static final Logger LOG = LoggerFactory.getLogger(ParallelismProphet.class.getName());
  private final SimulationScheduler simulationScheduler;
  private final PhysicalPlanGenerator physicalPlanGenerator;
  private final IRDAG currentIRDAG;
  private final PhysicalPlan currentPhysicalPlan;
  private final Set<StageEdge> edgesToOptimize;
  private final Set<String> stageId;
  private int partitionerProperty;

  public ParallelismProphet(final IRDAG irdag, final PhysicalPlan physicalPlan,
                            final SimulationScheduler simulationScheduler,
                            final PhysicalPlanGenerator physicalPlanGenerator,
                            final Set<StageEdge> edgesToOptimize) {
    this.currentIRDAG = irdag;
    this.currentPhysicalPlan = physicalPlan;
    this.simulationScheduler = simulationScheduler;
    this.physicalPlanGenerator = physicalPlanGenerator;
    this.edgesToOptimize = edgesToOptimize;
    this.stageId = edgesToOptimize.stream().map(StageEdge::getDst).map(Stage::getId).collect(Collectors.toSet());
    calculatePartitionerProperty(edgesToOptimize);
  }
  //task size ratio is same as parallelism
  private Pair<Integer, Long> launchSimulationForPlan(final PhysicalPlan physicalPlan) {
    this.simulationScheduler.schedulePlan(physicalPlan, 1);
    final MetricStore resultingMetricStore = this.simulationScheduler.collectMetricStore();
    final List<Pair<Integer, Long>> taskSizeRatioToDuration = new ArrayList<>();
    resultingMetricStore.getMetricMap(JobMetric.class).values().forEach(jobMetric -> {
      final int taskSizeRatio = Integer.parseInt(((JobMetric) jobMetric).getId().split("-")[1]);
      taskSizeRatioToDuration.add(Pair.of(taskSizeRatio, ((JobMetric) jobMetric).getJobDuration()));
    });
    return Collections.min(taskSizeRatioToDuration, Comparator.comparing(Pair::right));
  }

  @Override
  public Map<String, Long> calculate() {
    final Map<String, Long> result = new HashMap<>();
    final List<PhysicalPlan> listOfPhysicalPlans = new ArrayList<>(); // when to update here?
    for (int i = 0; i < 7; i++) {
      final int parallelism = (int) (partitionerProperty / Math.pow(2, i));
      LOG.error("parallelism = {}", parallelism);
      PhysicalPlan newPlan = makePhysicalPlanForSimulation(parallelism, edgesToOptimize, currentIRDAG);
      listOfPhysicalPlans.add(newPlan);
    }
    // is this right?
    final List<Pair<Integer, Long>> listOfParallelismToDurationPair = listOfPhysicalPlans.stream()
      .map(this::launchSimulationForPlan)
      .filter(pair -> pair.right() > 0.5)
      .collect(Collectors.toList());
    LOG.error("list of pairs {}", listOfParallelismToDurationPair);

    final Pair<Integer, Long> pairWithMinDuration =
        Collections.min(listOfParallelismToDurationPair, Comparator.comparing(p -> p.right()));
    LOG.error("optimal pair {}", pairWithMinDuration);
    this.simulationScheduler.terminate();

    result.put("opt.parallelism", pairWithMinDuration.left().longValue());
    return result;
  }

  private void setPartitionerProperty(final int partitionerProperty) {
    this.partitionerProperty = partitionerProperty;
  }
  private void calculatePartitionerProperty(final Set<StageEdge> edges) {
    setPartitionerProperty(edges.iterator().next().getPropertyValue(PartitionerProperty.class).get().right());
  }

  private PhysicalPlan makePhysicalPlanForSimulation(final int parallelism,
                                                     final Set<StageEdge> edges,
                                                     final IRDAG currentDag) {
    Set<IRVertex> verticesToChangeParallelism = edges.stream()
      .map(edge -> edge.getDst().getIRDAG().getVertices())
      .flatMap(list -> list.stream()).collect(Collectors.toSet());
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    // make dag of only one stage (targetStage)
    currentDag.topologicalDo(v -> {
      if (verticesToChangeParallelism.contains(v)) {
        v.setProperty(ParallelismProperty.of(parallelism));
        dagBuilder.addVertex(v);
        for (IREdge edge : currentDag.getIncomingEdgesOf(v)) {
          if (verticesToChangeParallelism.contains(edge.getSrc())) {
            dagBuilder.connectVertices(edge);
          }
        }
      }
    });
    final IRDAG newDag = new IRDAG(dagBuilder.build());
    final DAG<Stage, StageEdge> stageDag = physicalPlanGenerator.stagePartitionIrDAG(newDag);
    return new PhysicalPlan(currentPhysicalPlan.getPlanId().concat("-" + parallelism), stageDag);
  }
}
