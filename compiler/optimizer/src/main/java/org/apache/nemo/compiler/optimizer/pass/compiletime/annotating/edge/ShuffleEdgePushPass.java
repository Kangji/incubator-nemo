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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.edge;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

import java.util.List;

/**
 * A pass for tagging shuffle edges different from the default ones.
 * It sets DataFlowModel ExecutionProperty as "push".
 */
@Annotates(DataFlowProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class ShuffleEdgePushPass extends AnnotatingPass<IREdge> {
  /**
   * Default constructor.
   */
  public ShuffleEdgePushPass() {
    super(ShuffleEdgePushPass.class);
    this.addToRuleSet(EdgeRule.of(
      (IREdge edge) -> CommunicationPatternProperty.Value.SHUFFLE
        .equals(edge.getPropertyValue(CommunicationPatternProperty.class).orElse(null)),
      (IREdge edge) -> edge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.PUSH))));
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(irVertex -> dag.getIncomingEdgesOf(irVertex).forEach(irEdge ->
      this.getRuleSet().forEach(rule -> {
        if (rule.getCondition().test(irEdge)) {
          rule.getAction().accept(irEdge);
        }
      })));
    return dag;
  }
}
