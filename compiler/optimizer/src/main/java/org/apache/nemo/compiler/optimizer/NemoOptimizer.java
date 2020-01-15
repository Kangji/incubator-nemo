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
package org.apache.nemo.compiler.optimizer;

import com.google.common.annotations.VisibleForTesting;
import net.jcip.annotations.NotThreadSafe;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CacheIDProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.CachedSourceVertex;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.IgnoreSchedulingTempDataReceiverProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;
import org.apache.nemo.compiler.optimizer.policy.Policy;
import org.apache.nemo.compiler.optimizer.policy.XGBoostPolicy;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.ClientRPC;
import org.apache.nemo.runtime.common.metric.MetricUtils;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * An interface for optimizer, which manages the optimization over submitted IR DAGs through {@link Policy}s.
 * The instance of this class will reside in driver.
 */
@NotThreadSafe
public final class NemoOptimizer implements Optimizer {
  private final String dagDirectory;
  private final Policy optimizationPolicy;
  private final String environmentTypeStr;
  private final Integer desiredSourceParallelism;
  private final String executorInfoPath;
  private final String executorInfoContents;
  private final ClientRPC clientRPC;

  private final Map<UUID, Integer> cacheIdToParallelism = new HashMap<>();


  /**
   * @param dagDirectory       to store JSON representation of intermediate DAGs.
   * @param policyName         the name of the optimization policy.
   * @param environmentTypeStr the environment type of the workload to optimize the DAG for.
   * @param desiredSourceParallelism the desired source parallelism.
   * @param executorInfoPath   the string containing the information about the executors provided.
   * @param executorInfoContents the string of the information of the executors provided, in xml or json.
   * @param clientRPC          the RPC channel to communicate with the client.
   */
  @Inject
  private NemoOptimizer(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                        @Parameter(JobConf.OptimizationPolicy.class) final String policyName,
                        @Parameter(JobConf.EnvironmentType.class) final String environmentTypeStr,
                        @Parameter(JobConf.DesiredSourceParallelism.class) final Integer desiredSourceParallelism,
                        @Parameter(JobConf.ExecutorInfoPath.class) final String executorInfoPath,
                        @Parameter(JobConf.ExecutorInfoContents.class) final String executorInfoContents,
                        final ClientRPC clientRPC) {
    this.dagDirectory = dagDirectory;
    this.environmentTypeStr = OptimizerUtils.filterEnvironmentTypeString(environmentTypeStr);
    this.desiredSourceParallelism = desiredSourceParallelism;
    this.executorInfoPath = executorInfoPath;
    this.executorInfoContents = executorInfoContents;
    this.clientRPC = clientRPC;

    try {
      optimizationPolicy = (Policy) Class.forName(policyName).newInstance();
      if (policyName == null) {
        throw new CompileTimeOptimizationException("A policy name should be specified.");
      }
    } catch (final Exception e) {
      throw new CompileTimeOptimizationException(e);
    }
  }

  @Override
  public IRDAG optimizeAtCompileTime(final IRDAG dag) {
    dag.storeJSON(dagDirectory, "ir-0-initial", "IR before optimization");

    final IRDAG optimizedDAG;

    // Set source parallelism first.
    final IRDAG sourceParallelismSetIRDAG = setSourceParallelism(dag, this.desiredSourceParallelism);

    // Handle caching prior to compile-time optimizations.
    final Map<UUID, IREdge> cacheIdToEdge = new HashMap<>();
    final IRDAG cacheFilteredDag = handleCaching(sourceParallelismSetIRDAG, cacheIdToEdge);
    if (!cacheIdToEdge.isEmpty()) {
      cacheFilteredDag.storeJSON(dagDirectory, "ir-0-FilterCache",
        "IR after cache filtering");
    }

    // Conduct compile-time optimization.
    beforeCompileTimeOptimization(dag, optimizationPolicy);
    optimizedDAG = optimizationPolicy.runCompileTimeOptimization(cacheFilteredDag, dagDirectory);
    optimizedDAG
      .storeJSON(dagDirectory, "ir-afterpolicy-" + optimizationPolicy.getClass().getSimpleName(),
        "IR optimized for " + optimizationPolicy.getClass().getSimpleName());

    // Update cached list.
    // TODO #191: Report the actual state of cached data to optimizer.
    // Now we assume that the optimized dag always run properly.
    cacheIdToEdge.forEach((cacheId, edge) -> {
      if (!cacheIdToParallelism.containsKey(cacheId)) {
        cacheIdToParallelism.put(
          cacheId, optimizedDAG
            .getVertexById(edge.getDst().getId()).getPropertyValue(ParallelismProperty.class)
            .orElseThrow(() -> new RuntimeException("No parallelism on an IR vertex.")));
      }
    });

    // Return optimized dag
    return optimizedDAG;
  }

  @Override
  public IRDAG optimizeAtRunTime(final IRDAG dag, final Message message) {
    return optimizationPolicy.runRunTimeOptimizations(dag, message);
  }

  /**
   * Operations to be done prior to the Compile-Time Optimizations.
   * TODO #371: This part can be reduced by not using the client RPC and sending the python script to the driver
   * itself later on.
   *
   * @param dag    the DAG to process.
   * @param policy the optimization policy to optimize the DAG with.
   */
  private void beforeCompileTimeOptimization(final IRDAG dag, final Policy policy) {
    dag.recordExecutorInfo(OptimizerUtils.parseResourceSpecificationString(this.executorInfoContents));
    if (policy instanceof XGBoostPolicy) {
      if (this.dagDirectory != null || this.dagDirectory != "") {
        final String properties = MetricUtils.stringifyIRDAGProperties(dag);
        final File file = new File(this.dagDirectory, "ir-initial-properties.json");
        file.getParentFile().mkdirs();
        try (PrintWriter printWriter = new PrintWriter(file)) {
          printWriter.println(properties);
        } catch (IOException e) {
          // ignore
        }
      }

      clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
        .setType(ControlMessage.DriverToClientMessageType.LaunchOptimization)
        .setOptimizationType(ControlMessage.OptimizationType.XGBoost)
        .setDataCollected(ControlMessage.DataCollectMessage.newBuilder()
          .setData(" -d " + new File(this.dagDirectory).getAbsolutePath() + " -r " + this.executorInfoPath)
          .build())
        .build());
    }
  }

  /**
   * Set the initial source parallelism.
   *
   * @param dag the original IR DAG.
   * @return the IR DAG with the source parallelism set.
   */
  @VisibleForTesting
  public static IRDAG setSourceParallelism(final IRDAG dag, final Integer desiredSourceParallelism) {
    dag.getVertices().stream()
      .filter(v -> dag.getIncomingEdgesOf(v).isEmpty() && v instanceof SourceVertex)
      .forEach(v -> {
        try {
          // For source vertices, we try to split the source reader by the desired source parallelism.
          // After that, we set the parallelism as the number of split readers.
          // (It can be more/less than the desired value.)
          final SourceVertex sourceVertex = (SourceVertex) v;
          final int sourceParallelism;
          if (desiredSourceParallelism == 0) {
            final long estimatedSizeBytes = sourceVertex.getEstimatedSizeBytes();

            if (estimatedSizeBytes < 64 * 1024 * 1024) {  // less than 64 MB.
              sourceParallelism = sourceVertex.getReadables(1).size();  // 1 block.
            } else if (estimatedSizeBytes < 16 * 1024 * 1024 * 1024L) {  // less than 16 GB.
              sourceParallelism = sourceVertex.getReadables(
                (int) (estimatedSizeBytes / 64 * 1024 * 1024)).size();  // partition in 64MB blocks [1, 256].
            } else {  // greater than 16 GB.
              sourceParallelism = sourceVertex.getReadables(
                (int) (estimatedSizeBytes / 256 * 1024 * 1024)).size();  // partition in 256MB blocks [64, ].
            }
          } else {
            sourceParallelism = sourceVertex.getReadables(desiredSourceParallelism).size();
          }
          sourceVertex.setPropertyPermanently(ParallelismProperty.of(sourceParallelism));
        } catch (Exception e) {
          throw new CompileTimeOptimizationException(e);
        }
      });
    return dag;
  }

  /**
   * Handle data caching.
   * At first, it search the edges having cache ID from the given dag and update them to the given map.
   * Then, if some edge of a submitted dag is annotated as "cached" and the data was produced already,
   * the part of the submitted dag which produces the cached data will be cropped and the last vertex
   * before the cached edge will be replaced with a cached data source vertex.
   * This cached edge will be detected and appended to the original dag in scheduler.
   *
   * @param dag           the dag to handle.
   * @param cacheIdToEdge the map from cache ID to edge to update.
   * @return the cropped dag regarding to caching.
   */
  private IRDAG handleCaching(final IRDAG dag, final Map<UUID, IREdge> cacheIdToEdge) {
    dag.topologicalDo(irVertex ->
      dag.getIncomingEdgesOf(irVertex).forEach(
        edge -> edge.getPropertyValue(CacheIDProperty.class).
          ifPresent(cacheId -> cacheIdToEdge.put(cacheId, edge))
      ));

    if (cacheIdToEdge.isEmpty()) {
      return dag;
    } else {
      final DAGBuilder<IRVertex, IREdge> filteredDagBuilder = new DAGBuilder<>();
      final List<IRVertex> sinkVertices = dag.getVertices().stream()
        .filter(irVertex -> dag.getOutgoingEdgesOf(irVertex).isEmpty())
        .collect(Collectors.toList());
      sinkVertices.forEach(filteredDagBuilder::addVertex); // Sink vertex cannot be cached already.

      sinkVertices.forEach(sinkVtx -> addNonCachedVerticesAndEdges(dag, sinkVtx, filteredDagBuilder));

      return new IRDAG(filteredDagBuilder.buildWithoutSourceCheck());
    }
  }

  /**
   * Recursively add vertices and edges after cached edges to the dag builder in reversed order.
   *
   * @param dag      the original dag to filter.
   * @param irVertex the ir vertex to consider to add.
   * @param builder  the filtered dag builder.
   */
  private void addNonCachedVerticesAndEdges(final IRDAG dag,
                                            final IRVertex irVertex,
                                            final DAGBuilder<IRVertex, IREdge> builder) {
    if (irVertex.getPropertyValue(IgnoreSchedulingTempDataReceiverProperty.class).orElse(false)
      && dag.getIncomingEdgesOf(irVertex).stream()
      .filter(irEdge -> irEdge.getPropertyValue(CacheIDProperty.class).isPresent())
      .anyMatch(irEdge -> cacheIdToParallelism
        .containsKey(irEdge.getPropertyValue(CacheIDProperty.class).get()))) {
      builder.removeVertex(irVertex); // Ignore ghost vertex which was cached once.
      return;
    }

    dag.getIncomingEdgesOf(irVertex).stream()
      .forEach(edge -> {
        final Optional<UUID> cacheId = dag.getOutgoingEdgesOf(edge.getSrc()).stream()
          .filter(edgeToFilter -> edgeToFilter.getPropertyValue(CacheIDProperty.class).isPresent())
          .map(edgeToMap -> edgeToMap.getPropertyValue(CacheIDProperty.class).get())
          .findFirst();
        if (cacheId.isPresent() && cacheIdToParallelism.get(cacheId.get()) != null) { // Cached already.
          // Replace the vertex emitting cached edge with a cached source vertex.
          final IRVertex cachedDataRelayVertex = new CachedSourceVertex(cacheIdToParallelism.get(cacheId.get()));
          cachedDataRelayVertex.setPropertyPermanently(
            ParallelismProperty.of(cacheIdToParallelism.get(cacheId.get())));

          builder.addVertex(cachedDataRelayVertex);
          final IREdge newEdge = new IREdge(
            edge.getPropertyValue(CommunicationPatternProperty.class)
              .orElseThrow(() -> new RuntimeException("No communication pattern on an ir edge")),
            cachedDataRelayVertex,
            irVertex);
          edge.copyExecutionPropertiesTo(newEdge);
          newEdge.setProperty(CacheIDProperty.of(cacheId.get()));
          builder.connectVertices(newEdge);
          // Stop the recursion for this vertex.
        } else {
          final IRVertex srcVtx = edge.getSrc();
          builder.addVertex(srcVtx);
          builder.connectVertices(edge);
          addNonCachedVerticesAndEdges(dag, srcVtx, builder);
        }
      });
  }
}
