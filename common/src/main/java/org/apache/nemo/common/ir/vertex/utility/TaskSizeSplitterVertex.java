/*
Licensed to the Apache Software Foundation (ASF) under one
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
 *
 */

package org.apache.nemo.common.ir.vertex.utility;

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.test.EmptyComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This vertex works as a partition-based sampling vertex of dynamic task sizing pass.
 *
 * @Attributes
 * originalVertex:
 * partitionerProperty:
 * sumOfAllPartitions:
 * listOfPartitionSize:
 *
 * IRedge에 partition range 집어넣기
 */
public final class TaskSizeSplitterVertex extends LoopVertex {
  // Information about original(before splitting) vertices
  private static final Logger LOG = LoggerFactory.getLogger(TaskSizeSplitterVertex.class.getName());
  private final Set<IRVertex> originalVertices;
  private final Map<IRVertex, IRVertex> mapOfOriginalVertexToClone = new HashMap<>();
  private final IRVertex stageOpeningVertex;
  private final IRVertex stageEndingVertex;

  // Information about partition sizes
  private final int partitionerProperty;

  // Information about split vertices
  private int testingTrial;
  private final List<List<Integer>> listOfGroupedTaskIndices = new ArrayList<>();

  // list? of sub vertices
  // need to insert new trigger vertex in here?
  // trigger vertex info: set<partitionKey>, parallelism, size,

  public TaskSizeSplitterVertex(final String splitterVertexName,
                                final Set<IRVertex> originalVertices,
                                final Set<IRVertex> stageOpeningVertices,
                                final IRVertex stageEndingVertex,
                                final int partitionerProperty) {
    super(splitterVertexName); // need to take care of here
    this.testingTrial = 0;
    this.originalVertices = originalVertices;
    this.partitionerProperty = partitionerProperty;

    if (stageOpeningVertices.size() != 1) {
      throw new IllegalArgumentException();
    }
    for (IRVertex original : originalVertices) {
      mapOfOriginalVertexToClone.putIfAbsent(original, original.getClone());
    }

    this.stageOpeningVertex = stageOpeningVertices.iterator().next();
    this.stageEndingVertex = stageEndingVertex;

  }

  public Set<IRVertex> getOriginalVertices() {
    return originalVertices;
  }

  public void insertWorkingVertices(final Set<IRVertex> stageVertices, final Set<IREdge> edgesInBetween) {
    stageVertices.forEach(vertex -> getBuilder().addVertex(vertex));
    edgesInBetween.forEach(edge -> getBuilder().connectVertices(edge));
  }

  /**
   * Inserts signal Vertex at the end of the iteration.
   * @param toInsert : SignalVertex to insert.
   */
  public void insertSignalVertex(final SignalVertex toInsert) {
    getBuilder().addVertex(toInsert);
    IREdge edgesToSignal = EmptyComponents.newDummyShuffleEdge(stageEndingVertex, toInsert);
    getBuilder().connectVertices(edgesToSignal);
    IREdge controlEdgeToBeginning = Util.createControlEdge(toInsert, stageOpeningVertex);
    addIterativeIncomingEdge(controlEdgeToBeginning);
  }
  /**
   * Need to be careful about utility vertices, because they do not appear in the last iteration.
   * @param dagBuilder DAGBuilder to add the unrolled iteration to.
   * @return Modified this object
   */
  public TaskSizeSplitterVertex unRollIteration(final DAGBuilder<IRVertex, IREdge> dagBuilder) {
    final HashMap<IRVertex, IRVertex> originalToNewIRVertex = new HashMap<>();
    final HashSet<IRVertex> originalUtilityVertices = new HashSet<>();
    final DAG<IRVertex, IREdge> dagToAdd = getDAG();

    decreaseMaxNumberOfIterations();
    // add the working vertex and its incoming edges to the dagBuilder.
    dagToAdd.topologicalDo(irVertex -> {
      LOG.error("[LoopUnrolling] this is {}", irVertex.getId());
      if (!(irVertex instanceof SignalVertex)) {
        final IRVertex newIrVertex = irVertex.getClone();
        setParallelismPropertyByTestingTrial(newIrVertex);
        originalToNewIRVertex.putIfAbsent(irVertex, newIrVertex);
        dagBuilder.addVertex(newIrVertex, dagToAdd);
        dagToAdd.getIncomingEdgesOf(irVertex).forEach(edge -> {
          final IRVertex newSrc = originalToNewIRVertex.get(edge.getSrc());
          final IREdge newIrEdge =
            new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(), newSrc, newIrVertex);
          edge.copyExecutionPropertiesTo(newIrEdge);
          setSubPartitionPropertyByTestingTrial(newIrEdge);
          dagBuilder.connectVertices(newIrEdge);
        });
      } else {
        originalUtilityVertices.add(irVertex);
      }
    });

    // process the initial DAG incoming edges for the first loop.
    LOG.error("[LoopUnrolling] incoming edges {}", getDagIncomingEdges());
    getDagIncomingEdges().forEach((dstVertex, irEdges) -> irEdges.forEach(edge -> {
      final IREdge newIrEdge = new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(),
        edge.getSrc(), originalToNewIRVertex.get(dstVertex));
      edge.copyExecutionPropertiesTo(newIrEdge);
      setSubPartitionPropertyByTestingTrial(newIrEdge);
      LOG.error("[LoopUnrolling] original {}, new {}", edge, newIrEdge);
      dagBuilder.connectVertices(newIrEdge);
    }));

    LOG.error("[LoopUnrolling] outgoing edges {}", getDagOutgoingEdges());
    getDagOutgoingEdges().forEach((srcVertex, irEdges) -> irEdges.forEach(edgeFromOriginal -> {
      LOG.error("[LoopUnrolling] {}", this.getEdgeWithInternalVertexToEdgeWithLoop());
      for (Map.Entry<IREdge, IREdge> entry : this.getEdgeWithInternalVertexToEdgeWithLoop().entrySet()) {
        if (entry.getKey().getId().equals(edgeFromOriginal.getId())) {
          final IREdge correspondingEdge = entry.getValue(); // edge to next splitter vertex
          LOG.error("[LoopUnrolling] corresponding edge {}", correspondingEdge);
          if (correspondingEdge.getDst() instanceof TaskSizeSplitterVertex) {
            TaskSizeSplitterVertex nextSplitter = (TaskSizeSplitterVertex) correspondingEdge.getDst();
            LOG.error("[LoopUnrolling] next splitter {}", nextSplitter);
            IRVertex dstVertex = edgeFromOriginal.getDst(); // vertex inside of next splitter vertex
            LOG.error("[LoopUnrolling]next internal vertex {}", dstVertex);
            List<IREdge> edgesToDelete = new ArrayList<>();
            List<IREdge> edgesToAdd = new ArrayList<>();
            for (IREdge edgeToDst : nextSplitter.getDagIncomingEdges().get(dstVertex)) {
              LOG.error("[LoopUnrolling] edge to dest {} its src {}", edgeToDst, edgeToDst.getSrc());
              if (edgeToDst.getSrc().getId().equals(srcVertex.getId())) {
                final IREdge newIrEdge = new IREdge(
                  edgeFromOriginal.getPropertyValue(CommunicationPatternProperty.class).get(),
                  originalToNewIRVertex.get(srcVertex),
                  edgeFromOriginal.getDst());
                edgesToDelete.add(edgeToDst);
                edgesToAdd.add(newIrEdge);
                final IREdge newLoopEdge = Util.cloneEdge(
                  correspondingEdge, newIrEdge.getSrc(), correspondingEdge.getDst());
                nextSplitter.mapEdgeWithLoop(newLoopEdge, newIrEdge);
              }
            }
            LOG.error("[LoopUnrolling] edges to delete {} delete if this is last iteration", edgesToDelete);
            LOG.error("[LoopUnrolling] edges to add {}", edgesToAdd);
            if (loopTerminationConditionMet()) {
              for (IREdge edgeToDelete : edgesToDelete) {
                nextSplitter.removeDagIncomingEdge(edgeToDelete);
                nextSplitter.removeNonIterativeIncomingEdge(edgeToDelete);
              }
            }
            for (IREdge edgeToAdd : edgesToAdd) {
              nextSplitter.addDagIncomingEdge(edgeToAdd);
              nextSplitter.addNonIterativeIncomingEdge(edgeToAdd);
            }
          } else {
            final IREdge newIrEdge = new IREdge(
              edgeFromOriginal.getPropertyValue(CommunicationPatternProperty.class).get(),
              originalToNewIRVertex.get(srcVertex), edgeFromOriginal.getDst());
            edgeFromOriginal.copyExecutionPropertiesTo(newIrEdge);
            dagBuilder.addVertex(edgeFromOriginal.getDst()).connectVertices(newIrEdge);
          }
        }
      }
    }));

    // if loop termination condition is false, add signal vertex
    if (!loopTerminationConditionMet()) {
      for (IRVertex helper : originalUtilityVertices) {
        final IRVertex newHelper = helper.getClone();
        originalToNewIRVertex.putIfAbsent(helper, newHelper);
        setParallelismPropertyByTestingTrial(newHelper);
        dagBuilder.addVertex(newHelper, dagToAdd);
        dagToAdd.getIncomingEdgesOf(helper).forEach(edge -> {
          final IRVertex newSrc = originalToNewIRVertex.get(edge.getSrc());
          final IREdge newIrEdge =
            new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(), newSrc, newHelper);
          edge.copyExecutionPropertiesTo(newIrEdge);
          dagBuilder.connectVertices(newIrEdge);
        });
      }
    }
    // process next iteration's DAG incoming edges, and add them as the next loop's incoming edges:
    // clear, as we're done with the current loop and need to prepare it for the next one.
    this.getDagIncomingEdges().clear();
    this.getNonIterativeIncomingEdges().forEach((dstVertex, irEdges) -> irEdges.forEach(this::addDagIncomingEdge));
    if (!loopTerminationConditionMet()) {
      this.getIterativeIncomingEdges().forEach((dstVertex, irEdges) -> irEdges.forEach(edge -> {
        final IREdge newIrEdge = new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(),
          originalToNewIRVertex.get(edge.getSrc()), dstVertex);
        edge.copyExecutionPropertiesTo(newIrEdge);
        this.addDagIncomingEdge(newIrEdge);
      }));
    }

    increaseTestingTrial();
    return this;
  }

  private void setParallelismPropertyByTestingTrial(final IRVertex irVertex) {
    if (testingTrial == 0) {
      irVertex.setPropertyPermanently(ParallelismProperty.of(32));
    } else {
      irVertex.setProperty(ParallelismProperty.of(1));
    }
  }

  public void printLogs() {
    LOG.error("");
    LOG.error("[Vertex] this is splitter vertex {}", this.getId());
    LOG.error("[Vertex] get dag incoming edges: {}", this.getDagIncomingEdges().entrySet());
    LOG.error("[Vertex] get dag iterative incoming edges: {}", this.getIterativeIncomingEdges().entrySet());
    LOG.error("[Vertex] get dag noniterative incoming edges: {}", this.getNonIterativeIncomingEdges().entrySet());
    LOG.error("[Vertex] get dag outgoing edges: {}", this.getDagOutgoingEdges().entrySet());
    LOG.error("[Vertex] get edge map with loop {}", this.getEdgeWithLoopToEdgeWithInternalVertex().entrySet());
    LOG.error("[Vertex] get edge map with internal vertex {}",
      this.getEdgeWithInternalVertexToEdgeWithLoop().entrySet());
    LOG.error("");
  }
  public void increaseTestingTrial() {
    testingTrial++;
  }

  public List<Integer> getSubVertexByTestingTrial() {
    return listOfGroupedTaskIndices.get(testingTrial);
  }

  private void setSubPartitionPropertyByTestingTrial(final IREdge edge) {
    final ArrayList<KeyRange> partitionSet = new ArrayList<>();
    int taskIndex = 0;
    if (testingTrial == 0) {
      for (int i = 0; i < 4; i++) {
        partitionSet.add(taskIndex, HashRange.of(i, i + 1));
        taskIndex++;
      }
      for (int groupStartingIndex = 4; groupStartingIndex < 512; groupStartingIndex *= 2) {
        int growingFactor = groupStartingIndex / 4;
        for (int startIndex = groupStartingIndex; startIndex < groupStartingIndex * 2; startIndex += growingFactor) {
          partitionSet.add(taskIndex, HashRange.of(startIndex, startIndex + growingFactor));
          taskIndex++;
        }
      }
      edge.setProperty(SubPartitionSetProperty.of(partitionSet));
    } else {
      partitionSet.add(0, HashRange.of(512, partitionerProperty)); // 31+testingTrial
      edge.setProperty(SubPartitionSetProperty.of(partitionSet));
    }
  }

  public IRVertex getStageOpeningVertex() {
    return stageOpeningVertex;
  }

  public IRVertex getStageEndingVertex() {
    return stageEndingVertex;
  }
}
