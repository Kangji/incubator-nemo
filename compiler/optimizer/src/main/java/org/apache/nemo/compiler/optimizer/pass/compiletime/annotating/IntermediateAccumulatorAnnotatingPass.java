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

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.IdManager;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.MessageIdEdgeProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.BarrierProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.MessageIdVertexProperty;
import org.apache.nemo.compiler.optimizer.pass.runtime.IntermediateAccumulatorInsertionPass;

import java.util.HashSet;
import java.util.List;

/**
 * Sets {@link BarrierProperty} for vertices that can be optimized with intermediate accumulation.
 */
@Annotates(BarrierProperty.class)
public final class IntermediateAccumulatorAnnotatingPass extends AnnotatingPass {
  /**
   * Constructor.
   */
  public IntermediateAccumulatorAnnotatingPass() {
    super(IntermediateAccumulatorAnnotatingPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG irdag) {
    irdag.topologicalDo(v -> {
      final List<IREdge> incomingEdges = irdag.getIncomingEdgesOf(v);
      if (incomingEdges.stream()
        .anyMatch(e -> CommunicationPatternProperty.Value.SHUFFLE
          .equals(e.getPropertyValue(CommunicationPatternProperty.class)
            .orElse(CommunicationPatternProperty.Value.ONE_TO_ONE)))) {
        // Generate a message ID for runtime optimization.
        final Integer runtimeOptimizationMessageID = IdManager.generateMessageId();
        v.setProperty(MessageIdVertexProperty.of(runtimeOptimizationMessageID));
        incomingEdges.forEach(e -> {
          final HashSet<Integer> msgEdgeIds = e.getPropertyValue(MessageIdEdgeProperty.class).orElse(new HashSet<>(0));
          msgEdgeIds.add(runtimeOptimizationMessageID);
          e.setProperty(MessageIdEdgeProperty.of(msgEdgeIds));
        });
        // Annotate the barrier property with the name of the runtime pass to execute.
        v.setProperty(BarrierProperty.of(IntermediateAccumulatorInsertionPass.class.getCanonicalName()));
      }
    });
    return irdag;
  }
}
