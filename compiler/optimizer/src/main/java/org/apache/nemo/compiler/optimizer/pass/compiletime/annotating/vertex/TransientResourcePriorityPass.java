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
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceTypeProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

import java.util.List;

/**
 * Place valuable computations on reserved resources, and the rest on transient resources.
 */
@Annotates(ResourceTypeProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class TransientResourcePriorityPass extends AnnotatingPass<IRVertex> {
  /**
   * Default constructor.
   */
  public TransientResourcePriorityPass() {
    super(TransientResourcePriorityPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (inEdges.isEmpty()) {
        vertex.setPropertyPermanently(ResourceTypeProperty.of(ResourceTypeProperty.TRANSIENT));
      } else {
        if (hasM2M(inEdges) || allO2OFromReserved(inEdges)) {
          vertex.setPropertyPermanently(ResourceTypeProperty.of(ResourceTypeProperty.RESERVED));
        } else {
          vertex.setPropertyPermanently(ResourceTypeProperty.of(ResourceTypeProperty.TRANSIENT));
        }
      }
    });
    return dag;
  }

  /**
   * Checks whether the irEdges have M2M relationship.
   *
   * @param irEdges irEdges to check.
   * @return whether of not any of them has M2M relationship.
   */
  private boolean hasM2M(final List<IREdge> irEdges) {
    return irEdges.stream().anyMatch(edge ->
      edge.getPropertyValue(CommunicationPatternProperty.class).get()
        .equals(CommunicationPatternProperty.Value.SHUFFLE));
  }

  /**
   * Checks whether the irEdges are all from reserved containers.
   *
   * @param irEdges irEdges to check.
   * @return whether of not they are from reserved containers.
   */
  private boolean allO2OFromReserved(final List<IREdge> irEdges) {
    return irEdges.stream()
      .allMatch(edge -> CommunicationPatternProperty.Value.ONE_TO_ONE.equals(
        edge.getPropertyValue(CommunicationPatternProperty.class).get())
        && edge.getSrc().getPropertyValue(ResourceTypeProperty.class).get().equals(
        ResourceTypeProperty.RESERVED));
  }
}
