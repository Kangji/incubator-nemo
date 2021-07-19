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
package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.executionproperty.AssociatedProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ShuffleExecutorSetProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.TaskIndexToExecutorIDProperty;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Compare shuffle executor set and the executor.
 */
@AssociatedProperty(ShuffleExecutorSetProperty.class)
public final class IntermediateAccumulatorSchedulingConstraint implements SchedulingConstraint {

  @Inject
  private IntermediateAccumulatorSchedulingConstraint() {
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    if (!task.getPropertyValue(ShuffleExecutorSetProperty.class).isPresent()) {
      return true;
    }

    final HashSet<String> dataLocationNodeNames = task.getTaskIncomingEdges().stream()
      .flatMap(e -> {
        final Collection<List<Pair<String, String>>> collection = e.getSrc()
          .getPropertyValue(TaskIndexToExecutorIDProperty.class).get().values();
        return collection.stream().map(lst -> lst.get(lst.size() - 1).right());
      }).collect(Collectors.toCollection(HashSet::new));
    final ArrayList<HashSet<String>> setsOfExecutors = task.getPropertyValue(ShuffleExecutorSetProperty.class).get();
    final List<HashSet<String>> usedSetsOfExecutors = setsOfExecutors.stream()
      .filter(hs -> dataLocationNodeNames.stream().anyMatch(hs::contains)).collect(Collectors.toList());
    final int numOfSets = usedSetsOfExecutors.size();
    final int idx = task.getTaskIdx();
    return usedSetsOfExecutors.get(idx % numOfSets).contains(executor.getNodeName());
  }
}
