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
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceTypeProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

/**
 * Transient resource pass for tagging edges with DataStore ExecutionProperty.
 */
@Annotates(DataStoreProperty.class)
@Requires(ResourceTypeProperty.class)
public final class TransientResourceDataStorePass extends AnnotatingPass<IREdge> {
  /**
   * Default constructor.
   */
  public TransientResourceDataStorePass() {
    super(TransientResourceDataStorePass.class);
    this.addToRuleSet(EdgeRule.of(
      (IREdge edge, IRDAG dag) -> fromTransientToReserved(edge) && !DataStoreProperty.Value.SERIALIZED_MEMORY_STORE
        .equals(edge.getPropertyValue(DataStoreProperty.class).orElse(null)),
      (IREdge edge, IRDAG dag) ->
        edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.MEMORY_STORE))));
    this.addToRuleSet(EdgeRule.of(
      (IREdge edge, IRDAG dag) -> fromReservedToTransient(edge),
      (IREdge edge, IRDAG dag) ->
        edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.LOCAL_FILE_STORE))));
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
   * checks if the edge is from transient container to a reserved container.
   *
   * @param irEdge edge to check.
   * @return whether or not the edge satisfies the condition.
   */
  static boolean fromTransientToReserved(final IREdge irEdge) {
    return ResourceTypeProperty.TRANSIENT
      .equals(irEdge.getSrc().getPropertyValue(ResourceTypeProperty.class).get())
      && ResourceTypeProperty.RESERVED
      .equals(irEdge.getDst().getPropertyValue(ResourceTypeProperty.class).get());
  }

  /**
   * checks if the edge is from reserved container to a transient container.
   *
   * @param irEdge edge to check.
   * @return whether or not the edge satisfies the condition.
   */
  static boolean fromReservedToTransient(final IREdge irEdge) {
    return ResourceTypeProperty.RESERVED
      .equals(irEdge.getSrc().getPropertyValue(ResourceTypeProperty.class).get())
      && ResourceTypeProperty.TRANSIENT
      .equals(irEdge.getDst().getPropertyValue(ResourceTypeProperty.class).get());
  }
}
