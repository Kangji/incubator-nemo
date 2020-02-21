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
package org.apache.nemo.compiler.optimizer.pass.runtime;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.apache.nemo.runtime.common.plan.PhysicalPlan;
//import org.apache.nemo.runtime.common.plan.PhysicalPlanGenerator;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public final class DynamicTaskSizingRunTimePass extends RunTimePass<Map<String, Map<String, byte[]>>> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicTaskSizingRunTimePass.class.getName());
  //private final PhysicalPlanGenerator physicalPlanGenerator;
  //private final PhysicalPlan physicalPlan;
  //private final SimulationScheduler simulationScheduler

  public DynamicTaskSizingRunTimePass() {

  }

  @Override
  public IRDAG apply(final IRDAG irdag, final Message<Map<String, Map<String, byte[]>>> mapMessage) {
    final Set<IREdge> edges = mapMessage.getExaminedEdges();
    LOG.info("Examined edges {}", edges.stream().map(IREdge::getId).collect(Collectors.toList()));

    final IREdge representativeEdge = edges.iterator().next();


    return null;
  }

  private int getOptimizedTaskSizeRatioFromMessage(final Message<Map<String, Map<String, byte[]>>> mapMessage) {
    //dummy code for now
    return 1;
  }
}
