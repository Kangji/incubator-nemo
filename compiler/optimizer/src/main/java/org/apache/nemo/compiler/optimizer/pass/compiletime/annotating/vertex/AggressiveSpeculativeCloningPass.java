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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.vertex;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ClonedSchedulingProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

/**
 * Speculative execution. (very aggressive, for unit tests)
 * TODO #200: Maintain Test Passes and Policies Separately
 */
@Annotates(ClonedSchedulingProperty.class)
public final class AggressiveSpeculativeCloningPass extends AnnotatingPass<IRVertex> {
  // Speculative execution policy.
  private static final double FRACTION_TO_WAIT_FOR = 0.00000001; // Aggressive
  private static final double MEDIAN_TIME_MULTIPLIER = 1.00000001; // Aggressive

  /**
   * Default constructor.
   */
  public AggressiveSpeculativeCloningPass() {
    super(AggressiveSpeculativeCloningPass.class);
    this.addToRuleSet(VertexRule.of(
      (IRVertex vertex, IRDAG dag) -> true,  // Apply the policy to ALL vertices
      (IRVertex vertex, IRDAG dag) -> vertex.setProperty(ClonedSchedulingProperty.of(
        new ClonedSchedulingProperty.CloneConf(FRACTION_TO_WAIT_FOR, MEDIAN_TIME_MULTIPLIER)))));
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(vertex -> this.getRuleSet().forEach(rule -> {
      if (rule.getCondition().test(vertex, dag)) {
        rule.getAction().accept(vertex, dag);
      }
    }));
    return dag;
  }
}
