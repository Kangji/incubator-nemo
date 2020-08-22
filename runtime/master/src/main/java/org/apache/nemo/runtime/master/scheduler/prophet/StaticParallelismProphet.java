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

package org.apache.nemo.runtime.master.scheduler.prophet;

import com.fasterxml.jackson.databind.JsonNode;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.scheduler.SimulatedTaskExecutor;
import org.apache.nemo.runtime.master.scheduler.SimulationScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * A prophet class for choosing the right parallelism,
 * which tells you the estimated optimal parallelisms for each vertex.
 */
public final class StaticParallelismProphet implements Prophet<String, Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(StaticParallelismProphet.class.getName());

  private final SimulationScheduler simulationScheduler;
  private DAG<Stage, StageEdge> currentStageDAG;
  private Integer totalCores;
  private static HashMap<String, Long> stageIDToDurationCache = new HashMap<>();

  public StaticParallelismProphet(final SimulationScheduler simulationScheduler) {
    this.simulationScheduler = simulationScheduler;
    this.totalCores = 64;  // todo: Fix later on.
  }

  @Override
  public Map<String, Integer> calculate() {
    final Integer degreeOfConfigurationSpace = 7;

    final List<PhysicalPlan> listOfPhysicalPlans = new ArrayList<>();  // list to fill up with physical plans.
    final DAG<Stage, StageEdge> dagOfStages = currentStageDAG;

    final List<Pair<String, DAG<Stage, StageEdge>>> stageDAGs =
      makePhysicalPlansForSimulation(0, degreeOfConfigurationSpace, dagOfStages);
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
      vertexIdToParallelism.put(stageIterator.next().getId(), Integer.valueOf(stageParallelism));
    }
    return vertexIdToParallelism;
  }

  /**
   * Set the value of the current IR DAG to optimize.
   * @param stageDAG the current Stage DAG to optimize.
   */
  public void setCurrentStageDAG(final DAG<Stage, StageEdge> stageDAG) {
    this.currentStageDAG = stageDAG;
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
        (totalCores * Math.pow(2, i));

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
          makePhysicalPlansForSimulation(0, i, restStageDAGBeforeOpt);
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

  public static long estimateDurationOf(final Task task,
                                        final JobMetric jobMetric,
                                        final DAG<IRVertex, RuntimeEdge<IRVertex>> stageIRDAG) {
    final String taskStageID = task.getStageId();
    if (stageIDToDurationCache.containsKey(taskStageID)) {
      return stageIDToDurationCache.get(taskStageID);
    }

    final String[] irDAGSummary = jobMetric.getIrDagSummary().split("_");
    final Integer sourceNum = Integer.valueOf(irDAGSummary[0].split("rv")[1]);
    final Integer totalVerticesNum = Integer.valueOf(irDAGSummary[1].split("v")[1]);
    final Integer totalEdgeNum = Integer.valueOf(irDAGSummary[2].split("e")[1]);
    final Long inputSize = jobMetric.getInputSize();
    final Integer taskParallelism = task.getPropertyValue(ParallelismProperty.class).orElse(0);
    final Integer taskVertexCount = stageIRDAG.getVertices().size();
    final Integer scheduleGroup = task.getPropertyValue(ScheduleGroupProperty.class).orElse(0);

    final JsonNode irDAGJson = jobMetric.getIRDAG();
    final Integer numOfExecutors = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
      irDAGJson.get("executor_info").iterator(), Spliterator.ORDERED), false)
      .mapToInt(ei -> ei.get("count").asInt())
      .sum();
    final Long totalMemory = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
      irDAGJson.get("executor_info").iterator(), Spliterator.ORDERED), false)
      .mapToInt(ei -> ei.get("memory").asInt())
      .asLongStream().sum();
    final Integer totalCores = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
      irDAGJson.get("executor_info").iterator(), Spliterator.ORDERED), false)
      .mapToInt(ei -> ei.get("capacity").asInt())
      .sum();

    final List<StageEdge> incomingStageEdges = task.getTaskIncomingEdges();
    final Integer isSource = incomingStageEdges.size() > 0 ? 0 : 1;
    final Integer sourceCommunicationPatternShuffle = incomingStageEdges.stream().anyMatch(e ->
      e.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.SHUFFLE)) ? 1 : 0;
    final Integer sourceCommunicationPatternOneToOne = incomingStageEdges.stream().anyMatch(e ->
      e.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.ONE_TO_ONE)) ? 1 : 0;
    final Integer sourceCommunicationPatternBroadcast =  incomingStageEdges.stream().anyMatch(e ->
      e.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.BROADCAST)) ? 1 : 0;
    final Integer sourceDataStoreMemoryStore = incomingStageEdges.stream().anyMatch(e ->
      e.getDataStoreProperty().equals(DataStoreProperty.Value.MEMORY_STORE)
        || e.getDataStoreProperty().equals(DataStoreProperty.Value.SERIALIZED_MEMORY_STORE)) ? 1 : 0;
    final Integer sourceDataStoreLocalFileStore = incomingStageEdges.stream().anyMatch(e ->
      e.getDataStoreProperty().equals(DataStoreProperty.Value.LOCAL_FILE_STORE)) ? 1 : 0;
    final Integer sourceDataStoreDisaggStore = incomingStageEdges.stream().anyMatch(e ->
      e.getDataStoreProperty().equals(DataStoreProperty.Value.GLUSTER_FILE_STORE)) ? 1 : 0;
    final Integer sourceDataStorePipe = incomingStageEdges.stream().anyMatch(e ->
      e.getDataStoreProperty().equals(DataStoreProperty.Value.PIPE)) ? 1 : 0;
    final Integer sourceDataFlowPull = incomingStageEdges.stream().anyMatch(e ->
      e.getDataFlowModel().equals(DataFlowProperty.Value.PULL)) ? 1 : 0;
    final Integer sourceDataFlowPush = incomingStageEdges.stream().anyMatch(e ->
      e.getDataFlowModel().equals(DataFlowProperty.Value.PUSH)) ? 1 : 0;

    try {
      final String modelPath = "../../ml/task_duration_model/model/xgb.model";
      final File file = new File(modelPath);
      final Booster booster;
      if (file.exists()) {
        //reload model and data
        booster = XGBoost.loadModel(modelPath);
      } else {
        // Learn model
        final String trainPath = "../../ml/task_duration_model/dataset.txt.train";
        final String testPath = "../../ml/task_duration_model/dataset.txt.test";
        final DMatrix trainMat = new DMatrix(trainPath);
        final DMatrix testMat = new DMatrix(testPath);

        //specify parameters
        final HashMap<String, Object> params = new HashMap<String, Object>();
        params.put("eta", 0.8);
        params.put("max_depth", 6);
        params.put("verbosity", 1);
        params.put("objective", "reg:squarederror");

        final HashMap<String, DMatrix> watches = new HashMap<String, DMatrix>();
        watches.put("train", trainMat);
        watches.put("test", testMat);

        //set round
        final int round = 5;

        //train a boost model
        booster = XGBoost.train(trainMat, params, round, watches, null, null);

        //save model to modelPath
        final File path = new File("../../ml/task_duration_model/model");
        if (!path.exists()) {
          path.mkdirs();
        }
        booster.saveModel(modelPath);
      }

      // Format the data to predict the result for.
      final float[] data = new float[] {sourceNum, totalVerticesNum, totalEdgeNum, inputSize, numOfExecutors,
        totalMemory, totalCores, taskParallelism, taskVertexCount, scheduleGroup,
        isSource,
        sourceCommunicationPatternShuffle, sourceCommunicationPatternOneToOne, sourceCommunicationPatternBroadcast,
        sourceDataStoreMemoryStore, sourceDataStoreLocalFileStore, sourceDataStoreDisaggStore, sourceDataStorePipe,
        sourceDataFlowPull, sourceDataFlowPush
      };
      final int nrow = 1;
      final int ncol = 20;
      final float missing = 0.0f;
      final DMatrix currentData = new DMatrix(data, nrow, ncol, missing);
      // Predict using the trained booster model.
      final float[][] predicts = booster.predict(currentData);

      for (final float[] predict : predicts) {
        LOG.info("Prediction for {} is {}", task.getTaskId(), Arrays.toString(predict));
      }

      final Long result = (long) predicts[0][0];
      stageIDToDurationCache.putIfAbsent(taskStageID, result);
      return result.longValue();
    } catch (final XGBoostError e) {
      throw new CompileTimeOptimizationException(e);
    }
  }
}
