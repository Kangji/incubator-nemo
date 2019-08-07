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

package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceTypeProperty;
import org.apache.nemo.compiler.optimizer.OptimizerUtils;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.LargeShuffleCompositePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.TransientResourceCompositePass;
import org.apache.nemo.runtime.common.plan.StagePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A pass for annotating vertices requiring more memory resources with the memory ResourcePriorityProperty, then
 * conditionally running TransientResourcePass and LargeShufflePass, according to a specific condition in the DAG.
 */
@Annotates()
public final class ResourceExplorationPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceExplorationPass.class.getName());

  private static String resourceSpecificationString = "";

  /**
   * Default constructor.
   */
  public ResourceExplorationPass() {
    super(ResourceExplorationPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    IRDAG resultingDAG;
    final Map<IRVertex, Integer> vertexStageIdMap = new StagePartitioner().apply(dag);
    final Map<Integer, Long> stageIdToMemoryEdgeNumMap = new HashMap<>();
    final List<DataStoreProperty.Value> memoryValues =
      Arrays.asList(DataStoreProperty.Value.MemoryStore, DataStoreProperty.Value.SerializedMemoryStore);
    final List<Pair<String, List<Integer>>> resourceSpecs =
      OptimizerUtils.parseResourceSpecificationString(resourceSpecificationString);

    vertexStageIdMap.forEach((v, stageId) -> {
      if (v.getPropertyValue(ResourcePriorityProperty.class).isPresent()) {
        return;
      }
      final long memoryEdgeNum = dag.getOutgoingEdgesOf(v).stream()
        .filter(e -> memoryValues.contains(e.getPropertyValue(DataStoreProperty.class)
          .orElse(DataStoreProperty.Value.LocalFileStore)))
        .count();
      stageIdToMemoryEdgeNumMap.compute(stageId, (k, val) -> val == null ? memoryEdgeNum : val + memoryEdgeNum);
    });

    final List<Integer> memorySpecs = resourceSpecs.stream()
      .map(spec -> spec.right().get(0)).collect(Collectors.toList());

    final Integer maxMemory = memorySpecs.stream().mapToInt(i -> i).max().orElseThrow(() ->
      new CompileTimeOptimizationException("no executor supplied"));
    final Integer totalNumOfContainers = memorySpecs.size();
    final Long numOfMaxContainersWithMaxMemory = memorySpecs.stream().filter(i -> i.equals(maxMemory)).count();
    final Long numOfStagesToPlaceOnMaxMemoryContainers =
      stageIdToMemoryEdgeNumMap.size() * numOfMaxContainersWithMaxMemory / totalNumOfContainers;

    final Set<Integer> stagesRequiringMoreMemory = stageIdToMemoryEdgeNumMap.entrySet().stream()
      .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
      .limit(numOfStagesToPlaceOnMaxMemoryContainers)
      .map(Map.Entry::getKey)
      .collect(Collectors.toSet());

    vertexStageIdMap.forEach((v, sid) -> {
      if (stagesRequiringMoreMemory.contains(sid)) {
        v.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.MEMORY));
      }
    });

    // Run LargeShufflePass when the input dataset exceeds 512GB
    if (dag.getInputSize() > 512 * 1024 * 1024 * 1024L) {
      resultingDAG = new LargeShuffleCompositePass().apply(dag);
    } else {
      resultingDAG = dag;
    }

    // Try randomly running TransientResourcePass when there are transient resources specified in the json.
    final List<String> typeSpecs = resourceSpecs.stream()
      .map(Pair::left)
      .collect(Collectors.toList());

    if (typeSpecs.stream().anyMatch(s -> s.equalsIgnoreCase(ResourceTypeProperty.TRANSIENT))
      && new Random().nextBoolean()) {
      resultingDAG = new TransientResourceCompositePass().apply(resultingDAG);
    }

    return resultingDAG;
  }

  /**
   * Method to provide the resource specification information to the compile time pass prior to running
   * optimization passes.
   * @param resrcSpecificationString the string containing the resource specification information.
   */
  public static void updateResourceSpecificationString(final String resrcSpecificationString) {
    resourceSpecificationString = resrcSpecificationString;
  }
}
