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
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.dag.Vertex;
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
import org.apache.reef.tang.InjectionFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
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
  private final CountDownLatch currentStageDAGReadyLatch;
  private DAG<Stage, StageEdge> currentStageDAG;
  private Integer totalCores;
  private static HashMap<String, Long> dataToDurationCache = new HashMap<>();

  public StaticParallelismProphet(final InjectionFuture<SimulationScheduler> simulationSchedulerInjectionFuture) {
    this.simulationScheduler = simulationSchedulerInjectionFuture.get();
    this.currentStageDAGReadyLatch = new CountDownLatch(1);
    this.totalCores = 64;  // todo: Fix later on.
  }

  @Override
  public Map<String, Integer> calculate() {
    final Integer degreeOfConfigurationSpace = 7;

    final DAG<Stage, StageEdge> dagOfStages = currentStageDAG;

    try {
      LOG.info("Waiting for the DAG to be ready!");
      this.currentStageDAGReadyLatch.await();
    } catch (final InterruptedException e) {
      LOG.warn("interrupted.. ", e);
      // clean up state...
      Thread.currentThread().interrupt();
    }

    final List<Map<String, Integer>> stageDAGMaps =
      makePhysicalPlansForSimulation(0, degreeOfConfigurationSpace, dagOfStages);
    LOG.debug("StageDAG Maps: {}", stageDAGMaps);
    final List<Pair<String, Long>> listOfParallelismToDurationPair = new ArrayList<>();
    stageDAGMaps.forEach(map -> {
      final PhysicalPlan plan = constructPhysicalPlan(currentStageDAG, map);
      final Pair<String, Long> planIdAndDuration = this.launchSimulationForPlan(plan);
      if (planIdAndDuration.right() > 0.5) {
        listOfParallelismToDurationPair.add(planIdAndDuration);
      }
    });

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
    this.currentStageDAGReadyLatch.countDown();
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
  private List<Map<String, Integer>> makePhysicalPlansForSimulation(final int parallelismDivisor,
                                                                    final int degreeOfConfigurationSpace,
                                                                    final DAG<Stage, StageEdge> dagOfStages) {
    final List<String> topologicalStages = dagOfStages.getTopologicalSort()
      .stream().map(Vertex::getId).collect(Collectors.toList());
    return makePhysicalPlansForSimulation(parallelismDivisor, degreeOfConfigurationSpace, topologicalStages);
  }

  /**
   * Make Physical plan which is to be launched in Simulation Scheduler in a recursive manner.
   * @param parallelismDivisor the divisor for dividing the maximum parallelism with 2 to the power of the divisor.
   * @param degreeOfConfigurationSpace the upper limit of the divisor.
   * @param topologicalStages the stage IDs in the topologically sorted order.
   * @return              Physical plans with consists of selected vertices with given parallelism.
   */
  private List<Map<String, Integer>> makePhysicalPlansForSimulation(final int parallelismDivisor,
                                                              final int degreeOfConfigurationSpace,
                                                              final List<String> topologicalStages) {
    final List<Map<String, Integer>> result = new ArrayList<>();

    LOG.debug("Making physical plan for stages with {}, for ranges {} to {}",
      topologicalStages, parallelismDivisor, degreeOfConfigurationSpace);
    final String firstStageId = topologicalStages.get(0);

    // We only handle the space with smaller parallelism than the source, e.g. 512 to 2, 512 to 4, upto 512 to 512.
    IntStream.range(parallelismDivisor, degreeOfConfigurationSpace).forEachOrdered(i -> {
      final int parallelism = (int)  // parallelism for the first stage
        (totalCores * Math.pow(2, i));

      if (topologicalStages.size() == 1) {
        LOG.debug("Reached the end! Adding {} with {}", firstStageId, parallelism);
        final Map<String, Integer> map = new HashMap<>();
        map.put(firstStageId, parallelism);
        result.add(map);
      } else {  // we recursively apply the function to the rest of the stages.
        LOG.debug("recursion occurring!");
        final List<Map<String, Integer>> resultOfRest =  // this is where recursion occurs
          makePhysicalPlansForSimulation(0, i + 1,
            topologicalStages.subList(1, topologicalStages.size()));
        // recursively accumulated DAGs are each added onto the list.

        resultOfRest.forEach(m -> m.put(firstStageId, parallelism));
        result.addAll(resultOfRest);
      }
    });
    return result;
  }

  private static PhysicalPlan constructPhysicalPlan(final DAG<Stage, StageEdge> dagOfStages,
                                                            final Map<String, Integer> idToParallelism) {
    final List<Stage> topologicalStages = dagOfStages.getTopologicalSort();
    final DAGBuilder<Stage, StageEdge> stageDAGBuilder = new DAGBuilder<>();

    topologicalStages.forEach(stage -> {
      stage.getExecutionProperties().put(ParallelismProperty.of(idToParallelism.get(stage.getId())));
      stageDAGBuilder.addVertex(stage);
      dagOfStages.getIncomingEdgesOf(stage).forEach(stageDAGBuilder::connectVertices);
    });

    final DAG<Stage, StageEdge> stageDAG = stageDAGBuilder.buildWithoutSourceSinkCheck();
    final String id = stageDAG.getTopologicalSort().stream()
      .map(Stage::getId)
      .map(idToParallelism::get)
      .map(String::valueOf)
      .collect(Collectors.joining("-"));
    return new PhysicalPlan(id, stageDAG);
  }

  public static long estimateDurationOf(final Task task,
                                        final JobMetric jobMetric,
                                        final DAG<IRVertex, RuntimeEdge<IRVertex>> stageIRDAG) {


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

    // Format the data to predict the result for.
    final float[] data = new float[] {sourceNum, totalVerticesNum, totalEdgeNum, inputSize, numOfExecutors,
      totalMemory, totalCores, taskParallelism, taskVertexCount, scheduleGroup,
      isSource,
      sourceCommunicationPatternShuffle, sourceCommunicationPatternOneToOne, sourceCommunicationPatternBroadcast,
      sourceDataStoreMemoryStore, sourceDataStoreLocalFileStore, sourceDataStoreDisaggStore, sourceDataStorePipe,
      sourceDataFlowPull, sourceDataFlowPush
    };
    final String dataString = IntStream.range(0, data.length)
      .mapToObj(i -> String.valueOf(data[i]))
      .collect(Collectors.joining("-"));

    if (dataToDurationCache.containsKey(dataString)) {
      return dataToDurationCache.get(dataString);
    }

    try {
      final String projectRootPath = Util.fetchProjectRootPath();
      final String modelPath = projectRootPath + "/ml/task_duration_model/model/xgb.model";
      final File file = new File(modelPath);
      LOG.info("Model path: {}", file.getAbsolutePath());
      final Booster booster;
      if (file.exists()) {
        //reload model and data
        LOG.info("Reloaded existing XGBoost model");
        booster = XGBoost.loadModel(modelPath);
      } else {
        // Learn model
        LOG.info("Learning a new XGBoost model..");
        final String trainPath = projectRootPath + "/ml/task_duration_model/dataset.txt.train";
        final String testPath = projectRootPath + "/ml/task_duration_model/dataset.txt.test";
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
        final File path = new File(projectRootPath + "/ml/task_duration_model/model");
        if (!path.exists()) {
          path.mkdirs();
        }
        booster.saveModel(modelPath);
      }

      final int nrow = 1;
      final int ncol = 20;
      final float missing = 0.0f;
      final DMatrix currentData = new DMatrix(data, nrow, ncol, missing);

      // Predict using the trained booster model.
      final float[][] predicts = booster.predict(currentData);

      for (final float[] predict : predicts) {
        LOG.info("Prediction for {} is {}", task.getTaskId(), Arrays.toString(predict));
      }

      final long result = (long) predicts[0][0];
      dataToDurationCache.putIfAbsent(dataString, result);
      return result;
    } catch (final XGBoostError e) {
      throw new CompileTimeOptimizationException(e);
    }
  }
}
