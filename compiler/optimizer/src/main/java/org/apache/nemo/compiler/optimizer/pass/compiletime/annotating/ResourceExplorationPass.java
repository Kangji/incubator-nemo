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

import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceImportanceProperty;
import org.apache.nemo.compiler.optimizer.OptimizerUtils;
import org.apache.nemo.runtime.common.plan.StagePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

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
    final Map<IRVertex, Integer> vertexStageIdMap = new StagePartitioner().apply(dag);
    final Map<Integer, Long> stageIdToMemoryEdgeNumMap = new HashMap<>();
    final List<DataStoreProperty.Value> memoryValues =
      Arrays.asList(DataStoreProperty.Value.MemoryStore, DataStoreProperty.Value.SerializedMemoryStore);

    vertexStageIdMap.forEach((v, stageId) -> {
      if (v.getPropertyValue(ResourceImportanceProperty.class).isPresent()) {
        return;
      }
      final long memoryEdgeNum = dag.getOutgoingEdgesOf(v).stream()
        .filter(e -> memoryValues.contains(e.getPropertyValue(DataStoreProperty.class)
          .orElse(DataStoreProperty.Value.LocalFileStore)))
        .count();
      stageIdToMemoryEdgeNumMap.compute(stageId, (k, val) -> val == null ? memoryEdgeNum : val + memoryEdgeNum);
    });

    final List<Integer> memorySpecs =
      OptimizerUtils.parseResourceSpecificationString(resourceSpecificationString).stream()
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
        v.setProperty(ResourceImportanceProperty.of(ResourceImportanceProperty.MEMORY));
      }
    });

    // TODO: Transient 같은 애들 있으면 TransientPass를 random하게 돌리기?
    // TODO: Large input 이면 Large Shuffle pass를 먼저 돌릴까? 나중에 돌릴까?

    return dag;
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
