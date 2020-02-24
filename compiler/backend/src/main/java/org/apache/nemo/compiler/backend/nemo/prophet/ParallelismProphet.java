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

package org.apache.nemo.compiler.backend.nemo.prophet;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.metric.TaskMetric;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.scheduler.SimulationScheduler;

import java.util.*;

/**
 * A prophet for Parallelism.
 */
public final class ParallelismProphet implements Prophet {
  private final SimulationScheduler simulationScheduler;

  public ParallelismProphet(final int messageId, final PhysicalPlan physicalPlan,
                            final SimulationScheduler simulationScheduler) {
    this.simulationScheduler = simulationScheduler;
  }

  private Pair<Integer, Long> launchSimulationForPlan(final PhysicalPlan physicalPlan) {
    this.simulationScheduler.schedulePlan(physicalPlan, 1);
    final MetricStore resultingMetricStore = this.simulationScheduler.collectMetricStore();
    final List<Pair<Integer, Long>> taskSizeRatioToDuration = new ArrayList<>();
    resultingMetricStore.getMetricMap(TaskMetric.class).values().forEach(taskMetric -> {
      taskSizeRatioToDuration.add(Pair.of(((TaskMetric) taskMetric).getTaskSizeRatio(),
        ((TaskMetric) taskMetric).getTaskDuration()));
    });
    final Pair<Integer, Long> optimalPair = Collections.min(taskSizeRatioToDuration,
      new Comparator<Pair<Integer, Long>>() {
      @Override
      public int compare(Pair<Integer, Long> o1, Pair<Integer, Long> o2) {
        if (o1.right() > o2.right()) {
          return 1;
        } else if (o1.right() < o2.right()) {
          return -1;
        } else {
          return 0;
        }
      }
    });
    return optimalPair;
  }

  @Override
  public Map<String, Long> calculate() {
    final Map<String, Long> result = new HashMap<>();
    final List<PhysicalPlan> listOfPhysicalPlans = new ArrayList<>();

    final Pair<Integer, Long> pairWithMinDuration =
      listOfPhysicalPlans.stream().map(this::launchSimulationForPlan).min(Comparator.comparing(p -> p.right())).get();

    this.simulationScheduler.terminate();

    result.put("opt.parallelism", pairWithMinDuration.left().longValue());
    return result;
  }
}
