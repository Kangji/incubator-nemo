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

package org.apache.nemo.compiler.optimizer.prophet;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.SamplingTaskSizingPass;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PhysicalPlanGenerator;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.scheduler.SimulatedTaskExecutor;
import org.apache.nemo.runtime.master.scheduler.SimulationScheduler;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A prophet class for choosing the right parallelism,
 * which tells you the estimated optimal parallelisms for each vertex.
 */
public final class StaticParallelismProphet implements Prophet<String, Integer> {
  private final SimulationScheduler simulationScheduler;
  private final PhysicalPlanGenerator physicalPlanGenerator;
  private IRDAG currentIRDAG;

  @Inject
  public StaticParallelismProphet(final InjectionFuture<SimulationScheduler> simulationSchedulerInjectionFuture,
                                  final PhysicalPlanGenerator physicalPlanGenerator) {
    this.simulationScheduler = simulationSchedulerInjectionFuture.get();
    this.physicalPlanGenerator = physicalPlanGenerator;
  }

  @Override
  public Map<String, Integer> calculate() {
    final Integer degreeOfConfigurationSpace = 7;

    final List<PhysicalPlan> listOfPhysicalPlans = new ArrayList<>();  // list to fill up with physical plans.
    final DAG<Stage, StageEdge> dagOfStages = physicalPlanGenerator.stagePartitionIrDAG(currentIRDAG);

    final List<Pair<String, DAG<Stage, StageEdge>>> stageDAGs =
      makePhysicalPlansForSimulation(1, degreeOfConfigurationSpace, dagOfStages);
    stageDAGs.forEach(stageDAGPair ->
      listOfPhysicalPlans.add(new PhysicalPlan(stageDAGPair.left(), stageDAGPair.right())));

    final List<Pair<String, Long>> listOfParallelismToDurationPair = listOfPhysicalPlans.stream()
      .map(this::launchSimulationForPlan)
      .filter(pair -> pair.right() > 0.5)
      .collect(Collectors.toList());
    final Pair<String, Long> pairWithMinDuration =
      Collections.min(listOfParallelismToDurationPair, Comparator.comparing(Pair::right));

    final String[] stageParallelisms = pairWithMinDuration.left().split("-");
    final Iterator<Stage> stageIterator = dagOfStages.getTopologicalSort().iterator();
    final Map<String, Integer> vertexIdToParallelism = new HashMap<>();
    for (final String stageParallelism: stageParallelisms) {
      stageIterator.next().getIRDAG().getVertices().forEach(v ->
        vertexIdToParallelism.put(v.getId(), Integer.valueOf(stageParallelism)));
    }
    return vertexIdToParallelism;
  }

  /**
   * Set the value of the current IR DAG to optimize.
   * @param currentIRDAG the current IR DAG to optimize.
   */
  public void setCurrentIRDAG(final IRDAG currentIRDAG) {
    this.currentIRDAG = currentIRDAG;
  }

  /**
   * Simulate the given physical plan.
   * @param physicalPlan      physical plan(with only one stage) to simulate
   * @return                  Pair of Integer and Long. Integer value indicates the simulated parallelism, and
   *                          Long value is simulated job(=stage) duration.
   */
  private synchronized Pair<String, Long> launchSimulationForPlan(final PhysicalPlan physicalPlan) {
    this.simulationScheduler.setTaskDurationEstimationMethod(SimulatedTaskExecutor.Type.ANALYTIC_ESTIMATION);
    this.simulationScheduler.schedulePlan(physicalPlan, 1);
    final MetricStore resultingMetricStore = this.simulationScheduler.collectMetricStore();
    final List<Pair<String, Long>> taskSizeRatioToDuration = new ArrayList<>();
    resultingMetricStore.getMetricMap(JobMetric.class).values().forEach(jobMetric -> {
      final String planId = ((JobMetric) jobMetric).getId();
      taskSizeRatioToDuration.add(Pair.of(planId, ((JobMetric) jobMetric).getJobDuration()));
    });
    return Collections.min(taskSizeRatioToDuration, Comparator.comparing(Pair::right));
  }

  /**
   * Make Physical plan which is to be launched in Simulation Scheduler in a recursive manner.
   * @param parallelismDivisor the divisor for dividing the maximum parallelism with 2 to the power of the divisor.
   * @param degreeOfConfigurationSpace the upper limit of the divisor.
   * @param dagOfStages the dag of stages to refer to.
   * @return              Physical plans with consists of selected vertices with given parallelism.
   */
  private List<Pair<String, DAG<Stage, StageEdge>>> makePhysicalPlansForSimulation(final int parallelismDivisor,
                                                            final int degreeOfConfigurationSpace,
                                                            final DAG<Stage, StageEdge> dagOfStages) {
    final List<Pair<String, DAG<Stage, StageEdge>>> result = new ArrayList<>();
    final List<Stage> topologicalStages = dagOfStages.getTopologicalSort();

    // We only handle the space with smaller parallelism than the source, e.g. 512 to 2, 512 to 4, upto 512 to 512.
    IntStream.range(parallelismDivisor, degreeOfConfigurationSpace).forEachOrdered(i -> {
      final int parallelism = (int)  // parallelism for the first stage
        (SamplingTaskSizingPass.getPartitionerPropertyByJobSize(currentIRDAG) / Math.pow(2, i));

      // process first stage.
      final Stage firstStage = topologicalStages.remove(0);
      firstStage.getExecutionProperties().put(ParallelismProperty.of(parallelism));
      final DAG<Stage, StageEdge> firstStageDAG = new DAGBuilder<Stage, StageEdge>()
        .addVertex(firstStage).buildWithoutSourceSinkCheck();

      if (topologicalStages.isEmpty()) {  // no more rest of the stages. (rest = not first)
        result.add(Pair.of(String.valueOf(i), firstStageDAG));
      } else {  // we recursively apply the function to the rest of the stages.
        final DAGBuilder<Stage, StageEdge> restStageBuilder = new DAGBuilder<>();
        // Below is used when we have to connect the rest back to the first.
        final List<StageEdge> edgesToRestStage = new ArrayList<>();
        topologicalStages.forEach(s -> {  // make a separate stage DAG for the rest of the stages.
          restStageBuilder.addVertex(s);
          dagOfStages.getIncomingEdgesOf(s).forEach(e -> {
            if (restStageBuilder.contains(e.getSrc())) {
              restStageBuilder.connectVertices(e);
            } else {
              edgesToRestStage.add(e);
            }
          });
        });
        final DAG<Stage, StageEdge> restStageDAGBeforeOpt = restStageBuilder.buildWithoutSourceSinkCheck();
        final List<Pair<String, DAG<Stage, StageEdge>>> restStageDAGs =  // this is where recursion occurs
          makePhysicalPlansForSimulation(i, degreeOfConfigurationSpace, restStageDAGBeforeOpt);
        // recursively accumulated DAGs are each added onto the list.
        restStageDAGs.forEach(restStageDAGPair -> {
          final DAG<Stage, StageEdge> restStageDAG = restStageDAGPair.right();
          final DAGBuilder<Stage, StageEdge> fullDAGBuilder = new DAGBuilder<>(firstStageDAG);
          restStageDAG.topologicalDo(s -> {
            fullDAGBuilder.addVertex(s);
            restStageDAG.getIncomingEdgesOf(s).forEach(fullDAGBuilder::connectVertices);
          });

          edgesToRestStage.forEach(fullDAGBuilder::connectVertices);
          result.add(Pair.of(String.valueOf(i) + "-" + restStageDAGPair.left(),
            fullDAGBuilder.buildWithoutSourceSinkCheck()));
        });
      }
    });
    return result;
  }
}
