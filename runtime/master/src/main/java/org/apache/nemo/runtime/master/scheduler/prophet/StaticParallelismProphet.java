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
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.SamplingTaskSizingPass;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.scheduler.SimulatedTaskExecutor;
import org.apache.nemo.runtime.master.scheduler.SimulationScheduler;
import org.apache.reef.io.data.loading.api.DataLoader;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

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

  public static long estimateDurationOf(final Task task,
                                        final JobMetric jobMetric,
                                        final DAG<IRVertex, RuntimeEdge<IRVertex>> stageIRDAG,
                                        final JsonNode irDAGJson) {
    final String[] irDAGSummary = jobMetric.getIrDagSummary().split("_");
    final Integer sourceNum = Integer.valueOf(irDAGSummary[0].split("rv")[1]);
    final Integer totalVerticesNum = Integer.valueOf(irDAGSummary[1].split("v")[1]);
    final Integer totalEdgeNum = Integer.valueOf(irDAGSummary[2].split("e")[1]);
    final Long inputSize = jobMetric.getInputSize();
    final Integer taskParallelism = task.getPropertyValue(ParallelismProperty.class).orElse(0);
    final Integer taskVertexCount = stageIRDAG.getVertices().size();
    final Integer scheduleGroup = task.getPropertyValue(ScheduleGroupProperty.class).orElse(0);
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

    try {
      // Learn model
      final String trainPath = "";
      final String testPath = "";
      final DMatrix trainMat = new DMatrix(trainPath);
      final DMatrix testMat = new DMatrix(testPath);

      //specify parameters
      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put("eta", 0.8);
      params.put("max_depth", 5);
      params.put("verbosity", 1);
      params.put("objective", "reg:squaredlogerror");

      HashMap<String, DMatrix> watches = new HashMap<String, DMatrix>();
      watches.put("train", trainMat);
      watches.put("test", testMat);

      //set round
      int round = 2;

      //train a boost model
      Booster booster = XGBoost.train(trainMat, params, round, watches, null, null);

      //predict using first 2 tree
      // float[][] leafindex = booster.predictLeaf(testMat, 2);
      // for (float[] leafs : leafindex) {
      //   System.out.println(Arrays.toString(leafs));
      // }

      //predict all trees
      // leafindex = booster.predictLeaf(testMat, 0);
      // for (float[] leafs : leafindex) {
      //   System.out.println(Arrays.toString(leafs));
      // }

      //predict
      float[][] predicts = booster.predict(testMat);

      //save model to modelPath
      File file = new File("./model");
      if (!file.exists()) {
        file.mkdirs();
      }

      String modelPath = "./model/xgb.model";
      booster.saveModel(modelPath);

      //dump model with feature map
      String[] modelInfos = booster.getModelDump("../../demo/data/featmap.txt", false);
      saveDumpModel("./model/dump.raw.txt", modelInfos);

      //save dmatrix into binary buffer
      testMat.saveBinary("./model/dtest.buffer");

      //reload model and data
      Booster booster2 = XGBoost.loadModel("./model/xgb.model");
      DMatrix testMat2 = new DMatrix("./model/dtest.buffer");
      float[][] predicts2 = booster2.predict(testMat2);


      //check the two predicts
      System.out.println(checkPredicts(predicts, predicts2));

      System.out.println("start build dmatrix from csr sparse data ...");
      //build dmatrix from CSR Sparse Matrix
      DataLoader.CSRSparseData spData = DataLoader.loadSVMFile("../../demo/data/agaricus.txt.train");

      DMatrix trainMat2 = new DMatrix(spData.rowHeaders, spData.colIndex, spData.data,
        DMatrix.SparseType.CSR);
      trainMat2.setLabel(spData.labels);

      //specify watchList
      HashMap<String, DMatrix> watches2 = new HashMap<String, DMatrix>();
      watches2.put("train", trainMat2);
      watches2.put("test", testMat2);
      Booster booster3 = XGBoost.train(trainMat2, params, round, watches2, null, null);
      float[][] predicts3 = booster3.predict(testMat2);

      //check predicts
      System.out.println(checkPredicts(predicts, predicts3));

      // TODO: Do something based on https://github.com/dmlc/xgboost/blob/master/jvm-packages/xgboost4j-example/src/main/java/ml/dmlc/xgboost4j/java/example/BasicWalkThrough.java
    } catch (final XGBoostError e) {
      throw new CompileTimeOptimizationException(e);
    } catch (final IOException e) {
      throw new CompileTimeOptimizationException(e);
    }

    return 0;
  }

  public static void saveDumpModel(String modelPath, String[] modelInfos) throws IOException {
    try{
      PrintWriter writer = new PrintWriter(modelPath, "UTF-8");
      for(int i = 0; i < modelInfos.length; ++ i) {
        writer.print("booster[" + i + "]:\n");
        writer.print(modelInfos[i]);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static boolean checkPredicts(float[][] fPredicts, float[][] sPredicts) {
    if (fPredicts.length != sPredicts.length) {
      return false;
    }

    for (int i = 0; i < fPredicts.length; i++) {
      if (!Arrays.equals(fPredicts[i], sPredicts[i])) {
        return false;
      }
    }

    return true;
  }
}
