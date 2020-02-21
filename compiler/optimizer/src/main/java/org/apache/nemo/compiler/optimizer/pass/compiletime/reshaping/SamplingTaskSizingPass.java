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
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.executionproperty.ResourceSpecification;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.EnableDynamicTaskSizingProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.MessageIdVertexProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.utility.SignalVertex;
import org.apache.nemo.common.ir.vertex.utility.TaskSizeSplitterVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Compiler pass for dynamic task size optimization. Happens only when the edge property is SHUFFLE.
 * If (size of given job) >= 1GB: enable dynamic task sizing optimization.
 * else:                          break.
 *
 *
 * @Attributes
 * PARTITIONER_PROPERTY_FOR_SMALL_JOB:  PartitionerProperty for jobs in range of [1GB, 10GB) size.
 * PARTITIONER_PROPERTY_FOR_MEDIUM_JOB: PartitionerProperty for jobs in range of [10GB, 100GB) size.
 * PARTITIONER_PROPERTY_FOR_BIG_JOB:    PartitionerProperty for jobs in range of [100GB, - ) size(No upper limit).
 *
 * source stage - non o2o edge - current stage
 * -> source stage - trigger - message aggregator - [curr stage - trigger - message aggregator]
 * where [] is a splitter vertex
 */
@Annotates({EnableDynamicTaskSizingProperty.class, PartitionerProperty.class, SubPartitionSetProperty.class,
ParallelismProperty.class})
public final class SamplingTaskSizingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(SamplingTaskSizingPass.class.getName());

  private static final int PARTITIONER_PROPERTY_FOR_SMALL_JOB = 1024;
  private static final int PARTITIONER_PROPERTY_FOR_MEDIUM_JOB = 2048;
  private static final int PARTITIONER_PROPERTY_FOR_BIG_JOB = 4096;

  /**
   * Default constructor.
   */
  public SamplingTaskSizingPass() {
    super(SamplingTaskSizingPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    boolean enableDynamicTaskSizing = true; //boolean enableDynamicTaskSizing = getEnableFromJobSize(dag);
    /*
    if (!enableDynamicTaskSizing) {
      dag.topologicalDo(v -> {
        v.setProperty(EnableDynamicTaskSizingProperty.of(enableDynamicTaskSizing));
        for (IREdge e : dag.getIncomingEdgesOf(v)) {
          if (!CommunicationPatternProperty.Value.ONE_TO_ONE.equals(
            e.getPropertyValue(CommunicationPatternProperty.class).get())) {
            final int partitionerProperty = e.getDst().getPropertyValue(ParallelismProperty.class).get();
            e.setPropertyPermanently(PartitionerProperty.of(
              e.getPropertyValue(PartitionerProperty.class).get().left(), partitionerProperty));
          }
        }
      });
      return dag;
    }
     */
    //set partitionerProperty by job data size
    final int partitionerProperty = setPartitionerProperty(dag);
    dag.topologicalDo(v -> {
     v.setProperty(EnableDynamicTaskSizingProperty.of(enableDynamicTaskSizing));
      for (IREdge e : dag.getIncomingEdgesOf(v)) {
        if (!CommunicationPatternProperty.Value.ONE_TO_ONE.equals(
          e.getPropertyValue(CommunicationPatternProperty.class).get())) {
          e.setPropertyPermanently(PartitionerProperty.of(
            e.getPropertyValue(PartitionerProperty.class).get().left(), partitionerProperty));
        }
      }
    });

    /* Step 2. Insert Splitter Vertex */
    dag.topologicalDo(v -> {
      if (dag.getOutgoingEdgesOf(v).isEmpty()) {
        final IRVertex stageEndingVertex = v;
        final Set<IRVertex> partitionAll = recursivelyBuildPartition(stageEndingVertex, dag);
        final Set<IRVertex> partitionSources = partitionAll.stream().filter(vertexInPartition ->
          !dag.getIncomingEdgesOf(vertexInPartition).stream()
            .map(Edge::getSrc)
            .anyMatch(partitionAll::contains)
        ).collect(Collectors.toSet());
        makeAndInsertSplitterVertex(dag, partitionAll, partitionSources, stageEndingVertex, partitionerProperty);
      } else {
        for (final IREdge e : dag.getIncomingEdgesOf(v)) {
          if (!CommunicationPatternProperty.Value.ONE_TO_ONE.equals(
            e.getPropertyValue(CommunicationPatternProperty.class).get())) {
            final IRVertex stageEndingVertex = e.getSrc();
            final Set<IRVertex> partitionAll = recursivelyBuildPartition(stageEndingVertex, dag);
            final Set<IRVertex> partitionSources = partitionAll.stream().filter(vertexInPartition ->
              !dag.getIncomingEdgesOf(vertexInPartition).stream()
                .map(Edge::getSrc)
                .anyMatch(partitionAll::contains)
            ).collect(Collectors.toSet());
            final IRVertex stageEndingVertex2 = e.getSrc();
            final boolean isSourcePartition = partitionAll.stream()
              .flatMap(vertexInPartition -> dag.getIncomingEdgesOf(vertexInPartition).stream())
              .map(Edge::getSrc)
              .allMatch(partitionAll::contains);
            if (isSourcePartition) {
              break;
            }
            makeAndInsertSplitterVertex(dag, partitionAll, partitionSources, stageEndingVertex2, partitionerProperty);
          }
        }
      }
    });
    return dag;
  }

  /**
   * get and set edges which come to stage vertices from outer sources by observing the dag. This wil be the
   * 'dagIncomingEdges' in Splitter vertex.
   * Edge case: When previous vertex(i.e. outer source) is also a splitter vertex. In this case, we need to get
   *            original(invisible) edges by hacking in to previous splitter vertex.
   * @param dag               dag to observe
   * @param partitionSources  stage vertices
   * @return                  edges from outside to stage vertices
   */
  private Set<IREdge> setEdgesFromOutsideToOriginal(final IRDAG dag,
                                                    final Set<IRVertex> partitionSources) {
    // if previous vertex is splitter vertex, add the last vertex of that splitter vertex in map
    HashSet<IREdge> fromOutsideToOriginal = new HashSet<>();
    for (IRVertex partitionSource : partitionSources) {
      for (IREdge edge : dag.getIncomingEdgesOf(partitionSource)) {
        if (edge.getSrc() instanceof TaskSizeSplitterVertex) {
          IRVertex originalInnerSource = ((TaskSizeSplitterVertex) edge.getSrc()).getStageEndingVertex();
          Set<IREdge> candidates = ((TaskSizeSplitterVertex) edge.getSrc()).
            getDagOutgoingEdges().get(originalInnerSource);
          candidates.stream().filter(edge2 -> edge2.getDst().equals(partitionSource))
            .forEach(fromOutsideToOriginal::add);
        } else {
          fromOutsideToOriginal.add(edge);
        }
      }
    }
    return fromOutsideToOriginal;
  }

  /**
   * set edges which come to splitter from outside sources. These edges have a one-to-one relationship with
   * edgesFromOutsideToOriginal.
   * @param dag                   dag to observe
   * @param toInsert              splitter vertex to insert
   * @param stageOpeningVertices  stage opening vertices
   * @return                      set of edges pointing at splitter vertex
   */
  private Set<IREdge> setEdgesFromOutsideToSplitter(final IRDAG dag,
                                                    final TaskSizeSplitterVertex toInsert,
                                                    final Set<IRVertex> stageOpeningVertices) {
    HashSet<IREdge> fromOutsideToSplitter = new HashSet<>();
    for (IRVertex partitionSource : stageOpeningVertices) {
      for (IREdge incomingEdge : dag.getIncomingEdgesOf(partitionSource)) {
        if (incomingEdge.getSrc() instanceof TaskSizeSplitterVertex) {
          TaskSizeSplitterVertex prevSplitter = (TaskSizeSplitterVertex) incomingEdge.getSrc();
          IREdge internalEdge = prevSplitter.getEdgeWithInternalVertex(incomingEdge);
          IREdge newIrEdge = Util.cloneEdge(incomingEdge, incomingEdge.getSrc(), toInsert);
          prevSplitter.mapEdgeWithLoop(newIrEdge, internalEdge);
          fromOutsideToSplitter.add(newIrEdge);
        } else {
          IREdge cloneOfIncomingEdge = Util.cloneEdge(incomingEdge, incomingEdge.getSrc(), toInsert);
          fromOutsideToSplitter.add(cloneOfIncomingEdge);
        }
      }
    }
    return fromOutsideToSplitter;
  }

  /**
   * should be called after EnableDynamicTaskSizingProperty is declared as true.
   * @param dag   IRDAG to get job input data size from
   * @return      partitioner property regarding job size
   */
  private int setPartitionerProperty(final IRDAG dag) {
    long jobSizeInBytes = dag.getInputSize();
    long jobSizeInGB = jobSizeInBytes / (1024 * 1024 * 1024);
    if (1 <= jobSizeInGB && jobSizeInGB < 10) {
      return PARTITIONER_PROPERTY_FOR_SMALL_JOB;
    } else if (10 <= jobSizeInGB && jobSizeInGB < 100) {
      return PARTITIONER_PROPERTY_FOR_MEDIUM_JOB;
    } else {
      return PARTITIONER_PROPERTY_FOR_BIG_JOB;
    }
  }

  private boolean getEnableFromJobSize(final IRDAG dag) {
    long jobSizeInBytes = dag.getInputSize();
    return jobSizeInBytes >= 1024 * 1024 * 1024;
  }

  private int getNumberOfTotalExecutorCores(final IRDAG dag) {
    List<Pair<Integer, ResourceSpecification>> executorInfo = dag.getExecutorInfo();
    int numberOFTotalExecutorCores = 0;
    Iterator<Pair<Integer, ResourceSpecification>> iterator = executorInfo.iterator();
    while (iterator.hasNext()) {
      numberOFTotalExecutorCores += iterator.next().right().getCapacity();
    }
    return numberOFTotalExecutorCores;
  }

  /**
   * build partition of stage.
   */
  private Set<IRVertex> recursivelyBuildPartition(final IRVertex curVertex, final IRDAG dag) {
    final Set<IRVertex> unionSet = new HashSet<>();
    unionSet.add(curVertex);
    for (final IREdge inEdge : dag.getIncomingEdgesOf(curVertex)) {
      if (CommunicationPatternProperty.Value.ONE_TO_ONE
        .equals(inEdge.getPropertyValue(CommunicationPatternProperty.class).orElseThrow(IllegalStateException::new))
        && DataStoreProperty.Value.MEMORY_STORE
        .equals(inEdge.getPropertyValue(DataStoreProperty.class).orElseThrow(IllegalStateException::new))
        && dag.getIncomingEdgesOf(curVertex).size() == 1) {
        unionSet.addAll(recursivelyBuildPartition(inEdge.getSrc(), dag));
      }
    }
    return unionSet;
  }

  private void makeAndInsertSplitterVertex(final IRDAG dag,
                                           final Set<IRVertex> partitionAll,
                                           final Set<IRVertex> partitionSources,
                                           final IRVertex stageEndingVertex,
                                           final int partitionerProperty) {
    final Set<IREdge> incomingEdgesOfOriginalVertices = partitionAll
      .stream()
      .flatMap(ov -> dag.getIncomingEdgesOf(ov).stream())
      .collect(Collectors.toSet());
    final Set<IREdge> outgoingEdgesOfOriginalVertices = partitionAll
      .stream()
      .flatMap(ov -> dag.getOutgoingEdgesOf(ov).stream())
      .collect(Collectors.toSet());
    final Set<IREdge> edgesBetweenOriginalVertices = incomingEdgesOfOriginalVertices
      .stream()
      .filter(ovInEdge -> partitionAll.contains(ovInEdge.getSrc()))
      .collect(Collectors.toSet());
    final Set<IREdge> fromOutsideToOriginal = setEdgesFromOutsideToOriginal(dag, partitionSources);
    final Set<IREdge> fromOriginalToOutside = new HashSet<>(outgoingEdgesOfOriginalVertices);
    fromOriginalToOutside.removeAll(edgesBetweenOriginalVertices);
    final TaskSizeSplitterVertex toInsert = new TaskSizeSplitterVertex(
      "Splitter" + stageEndingVertex.getId(), partitionAll, partitionSources,
      stageEndingVertex, partitionerProperty);

    // By default, set the number of iterations as 2
    toInsert.setMaxNumberOfIterations(2);
    // make edges connected to splitter vertex
    final Set<IREdge> fromOutsideToSplitter = setEdgesFromOutsideToSplitter(dag, toInsert, partitionSources);
    final Set<IREdge> fromSplitterToOutside = new HashSet<>();
    fromOriginalToOutside.stream().map(outEdge -> Util.cloneEdge(
      outEdge,
      toInsert,
      outEdge.getDst()
    )).forEach(fromSplitterToOutside::add);

    final Set<IREdge> edgesWithSplitterVertex = new HashSet<>();
    edgesWithSplitterVertex.addAll(fromOutsideToSplitter);
    edgesWithSplitterVertex.addAll(fromSplitterToOutside);
    //fill in splitter vertex information
    toInsert.insertWorkingVertices(partitionAll, edgesBetweenOriginalVertices);

    //map splitter vertex connection to corresponding internal vertex connection
    for (IREdge splitterEdge : fromOutsideToSplitter) {
      for (IREdge internalEdge : fromOutsideToOriginal) {
        if (splitterEdge.getSrc() instanceof TaskSizeSplitterVertex) {
          TaskSizeSplitterVertex prevSplitter = (TaskSizeSplitterVertex) splitterEdge.getSrc();
          if (prevSplitter.getOriginalVertices().contains(internalEdge.getSrc())) {
            toInsert.mapEdgeWithLoop(splitterEdge, internalEdge);
          }
        } else {
          if (splitterEdge.getSrc().equals(internalEdge.getSrc())) {
            toInsert.mapEdgeWithLoop(splitterEdge, internalEdge);
          }
        }
      }
    }
    for (IREdge splitterEdge : fromSplitterToOutside) {
      for (IREdge internalEdge : fromOriginalToOutside) {
        if (splitterEdge.getDst().equals(internalEdge.getDst())) {
          toInsert.mapEdgeWithLoop(splitterEdge, internalEdge);
        }
      }
    }

    final SignalVertex signalVertex = new SignalVertex();

    for (IREdge edge : fromOutsideToOriginal) {
      final HashSet<Integer> msgEdgeIds =
        edge.getPropertyValue(MessageIdEdgeProperty.class).orElse(new HashSet<>(0));
      msgEdgeIds.add(signalVertex.getPropertyValue(MessageIdVertexProperty.class).get());
      edge.setProperty(MessageIdEdgeProperty.of(msgEdgeIds));
    }

    fromOutsideToOriginal.forEach(edge -> toInsert.addDagIncomingEdge(edge));
    fromOutsideToOriginal.forEach(edge -> toInsert.addNonIterativeIncomingEdge(edge)); //
    fromOriginalToOutside.forEach(edge -> toInsert.addDagOutgoingEdge(edge));

    toInsert.insertSignalVertex(signalVertex);
    // insert splitter vertex
    dag.insert(toInsert, incomingEdgesOfOriginalVertices, outgoingEdgesOfOriginalVertices,
      edgesWithSplitterVertex);

    toInsert.printLogs();
  }
}
