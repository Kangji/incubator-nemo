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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.exception.IllegalEdgeOperationException;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.utility.RelayVertex;
import org.apache.nemo.runtime.common.metric.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Pass for applying the best existing execution properties & DAG from the DB.
 */
@Annotates()
public final class BestInitialDAGConfFromDBPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(BestInitialDAGConfFromDBPass.class.getName());

  public BestInitialDAGConfFromDBPass() {
    super(BestInitialDAGConfFromDBPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    try (Connection c = DriverManager.getConnection(MetricUtils.POSTGRESQL_DB_NAME,
      "postgres", "fake_password")) {
      try (Statement statement = c.createStatement()) {
        statement.setQueryTimeout(30);  // set timeout to 30sec.

        try (ResultSet rs = statement.executeQuery("SELECT dag FROM " + dag.irDAGSummary()
          + " WHERE duration = (SELECT MIN (duration) FROM " + dag.irDAGSummary() + ")")) {
          LOG.info("Configuration loaded from DB");
          if (rs.next()) {
            final IRDAG bestDAG = SerializationUtils.deserialize(rs.getBytes("dag"));  // best dag.

//            LOG.info("best dag:" + bestDAG.getTopologicalSort());
//            LOG.info("curr dag:" + dag.getTopologicalSort());

            final Iterator<IRVertex> bestDAGVertices = bestDAG.getTopologicalSort().iterator();
            final Iterator<IRVertex> dagVertices = dag.getTopologicalSort().iterator();

            while (bestDAGVertices.hasNext() && dagVertices.hasNext()) {
              final IRVertex bestVertex = bestDAGVertices.next();

              if (bestVertex.isUtilityVertex()) {
                if (bestVertex instanceof RelayVertex) {
                  final List<IRVertex> srcVertices = bestDAG.getIncomingEdgesOf(bestVertex).stream()
                    .map(IREdge::getSrc).collect(Collectors.toList());
                  final List<IRVertex> dstVertices = bestDAG.getOutgoingEdgesOf(bestVertex).stream()
                    .map(IREdge::getDst).collect(Collectors.toList());

                  srcVertices.forEach(srcVertex ->
                    dstVertices.forEach(dstVertex -> {
                      try {
                        final IREdge edge = dag.getEdgeBetween(srcVertex.getId(), dstVertex.getId());

                        if (edge != null) {
                          dag.insert(new RelayVertex(), edge);
                        }
                      } catch (IllegalEdgeOperationException e) {
                        // ignore
                      }
                    }));
                }
              } else {
                final IRVertex irVertex = dagVertices.next();

                if (bestVertex.getClass().equals(irVertex.getClass())
                  && (!bestVertex.getClass().equals(OperatorVertex.class)
                  || bestVertex.toString().equals(irVertex.toString()))) {
                  bestVertex.copyExecutionPropertiesTo(irVertex);
                } else {
                  LOG.warn("DAG topology doesn't match while comparing {}({}) from the ideal DAG with {}({})",
                    bestVertex.getId(), bestVertex, irVertex.getId(), irVertex);
                }
              }
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new MetricException(e);
    }
    return dag;
  }
}
