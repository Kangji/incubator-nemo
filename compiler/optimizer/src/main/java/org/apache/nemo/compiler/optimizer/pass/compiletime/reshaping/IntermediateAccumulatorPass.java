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

package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.compiler.frontend.beam.transform.CombineTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A pass that inserts an intermediate accumulator in between combiners and reducers.
 * This pass is currently specific for BEAM applications, and has no effect on other applications.
 * This pass can be extended to other applications for it to take better effect.
 */
public final class IntermediateAccumulatorPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(IntermediateAccumulatorPass.class.getName());

  private final ArrayList<String> sourceExecutors;
  private final ArrayList<String> intermediateExecutors;

  /**
   * Default constructor.
   */
  public IntermediateAccumulatorPass(final ArrayList<String> sourceExecutors,
                                     final ArrayList<String> intermediateExecutors) {
    super(IntermediateAccumulatorPass.class);
    this.sourceExecutors = sourceExecutors;
    this.intermediateExecutors = intermediateExecutors;
  }

  @Override
  public IRDAG apply(final IRDAG irdag) {
    irdag.topologicalDo(v -> {
      if (v instanceof OperatorVertex && ((OperatorVertex) v).getTransform() instanceof CombineTransform
        && ((CombineTransform<?, ?, ?, ?>) ((OperatorVertex) v).getTransform()).isFinalCombining()) {
        // Insert an intermediate GBKTransform before the final combining vertex
        final List<IREdge> incomingShuffleEdges = irdag.getIncomingEdgesOf(v).stream()
          .filter(e -> CommunicationPatternProperty.Value.SHUFFLE
            .equals(e.getPropertyValue(CommunicationPatternProperty.class)
              .orElse(CommunicationPatternProperty.Value.ONE_TO_ONE)))
          .collect(Collectors.toList());

        final CombineTransform<?, ?, ?, ?> finalCombineStreamTransform =
          (CombineTransform<?, ?, ?, ?>) ((OperatorVertex) v).getTransform();
        final CombineTransform<?, ?, ?, ?> intermediateCombineStreamTransform =
          CombineTransform.getIntermediateCombineTransformOf(finalCombineStreamTransform);

        final OperatorVertex intermediateCombineOperatorVertex = new OperatorVertex(intermediateCombineStreamTransform);
        irdag.insert(intermediateCombineOperatorVertex, incomingShuffleEdges, sourceExecutors, intermediateExecutors);
      } // else if (v instanceof OperatorVertex && ((OperatorVertex) v).getTransform() instanceof GBKTransform) {
      // }
    });
    return irdag;
  }
}
