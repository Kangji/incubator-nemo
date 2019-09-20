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
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceTypeProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import org.apache.nemo.runtime.common.plan.StagePartitioner;

import static org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.edge.TransientResourceDataStorePass.fromTransientToReserved;

/**
 * Push from transient resources to reserved resources.
 */
@Annotates({DataFlowProperty.class, ResourceSlotProperty.class})
@Requires(ResourceTypeProperty.class)
public final class TransientResourceDataFlowPass extends AnnotatingPass<IREdge> {
  /**
   * Default constructor.
   */
  public TransientResourceDataFlowPass() {
    super(TransientResourceDataFlowPass.class);
    this.addToRuleSet(EdgeRule.of("TransientResourceDataFlow",
      (IREdge edge, IRDAG dag) -> fromTransientToReserved(edge),
      (IREdge edge, IRDAG dag)  -> {
        edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.PUSH));
        recursivelySetResourceSlotProperty(edge.getDst(), dag, false);
      }
    ));
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex).forEach(edge -> this.getRuleSet().forEach(rule -> {
      if (rule.getCondition().test(edge, dag)) {
        rule.getAction().accept(edge, dag);
      }
    })));
    return dag;
  }

  /**
   * Static method to recursively set the resource slot property to the vertices of the same stage.
   * @param v the vertex to start from.
   * @param dag the IRDAG to observe.
   * @param val the boolean value.
   */
  private static void recursivelySetResourceSlotProperty(final IRVertex v, final IRDAG dag, final boolean val) {
    dag.getOutgoingEdgesOf(v).forEach(e -> {
      if (StagePartitioner.testMergeability(e, dag)) {
        recursivelySetResourceSlotProperty(e.getDst(), dag, val);
      }
    });
    v.setPropertyPermanently(ResourceSlotProperty.of(val));
  }
}
