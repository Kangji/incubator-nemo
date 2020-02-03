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

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.utility.TaskSizeSplitterVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pass for unrolling the loops grouped by the {@link LoopExtractionPass}.
 * It first unrolls the root LoopVertex, which is in the form of linked list, into a straight line in the DAG.
 * Then, it decomposes each of the LoopVertices with the DAG information that each of them contain.
 */
public final class LoopUnrollingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(LoopUnrollingPass.class.getName());
  /**
   * Default constructor.
   */
  public LoopUnrollingPass() {
    super(LoopUnrollingPass.class);
    LOG.error("[HWARIM] loop unrolling pass init");
  }

  @Override
  public IRDAG apply(final IRDAG inputDAG) {
    inputDAG.reshapeUnsafely(dag -> {
      return recursivelyUnroll(dag);
    });
    return inputDAG;
  }

  /**
   * A function that recursively calls Unroll until there are no more LoopVertex left.
   *
   * @param dag DAG to process.
   * @return DAG without LoopVertex.
   */
  private DAG<IRVertex, IREdge> recursivelyUnroll(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    LOG.error("[HWARIM] search from here");
    dag.topologicalDo(irVertex -> {
      LOG.error("{} incoming edges {} outgoing edges {}", irVertex, dag.getIncomingEdgesOf(irVertex),
        dag.getOutgoingEdgesOf(irVertex));
      if (irVertex instanceof TaskSizeSplitterVertex) {
        TaskSizeSplitterVertex splitterVertex = (TaskSizeSplitterVertex) irVertex;
        splitterVertex.printLogs();
      }
    });

    dag.topologicalDo(irVertex -> {
      LOG.error("[HWARIM] this is {}", irVertex.getId());
      if (irVertex instanceof TaskSizeSplitterVertex) {
        TaskSizeSplitterVertex splitterVertex = (TaskSizeSplitterVertex) irVertex;
        splitterVertex.markDuplicateEdges();
        int i = 0;
        while (!splitterVertex.loopTerminationConditionMet()) {
          LOG.error("[HWARIM] {}, iteration {}", irVertex, i);
          splitterVertex = splitterVertex.unRollIteration(builder);
          i++;
        }
      } else if (irVertex instanceof LoopVertex) {
        LoopVertex loopVertex = (LoopVertex) irVertex;
        loopVertex.markDuplicateEdges();
        while (!loopVertex.loopTerminationConditionMet()) {
          loopVertex = loopVertex.unRollIteration(builder);
        }
      } else {
        builder.addVertex(irVertex, dag);
        dag.getIncomingEdgesOf(irVertex).forEach(e -> {
          if (!(e.getSrc() instanceof LoopVertex)) {
            builder.connectVertices(e);
          }
        });
      }
    });

    if (builder.contains(vertex -> vertex instanceof LoopVertex)) {
      return recursivelyUnroll(builder.build());
    } else {
      return builder.build();
    }
  }
}
