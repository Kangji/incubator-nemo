/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.AsBytesProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.util.Collections;

/**
 * A pass to support Sailfish-like shuffle by tagging edges.
 * This pass handles the AsBytes ExecutionProperty,
 * which makes {@link edu.snu.nemo.common.ir.vertex.transform.RelayTransform} to read and write data as bytes.
 */
public final class SailfishEdgeAsBytesPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public SailfishEdgeAsBytesPass() {
    super(ExecutionProperty.Key.AsBytes, Collections.singleton(ExecutionProperty.Key.DataCommunicationPattern));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      // Find the merger vertex inserted by reshaping pass.
      if (dag.getIncomingEdgesOf(vertex).stream().anyMatch(irEdge ->
              DataCommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)))) {
        dag.getIncomingEdgesOf(vertex).forEach(edgeToMerger -> {
          if (DataCommunicationPatternProperty.Value.Shuffle
          .equals(edgeToMerger.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
            // Read data as arrays of bytes, instead of deserialized Objects.
            edgeToMerger.setProperty(AsBytesProperty.of(AsBytesProperty.Value.ReadAsBytes));
          }
        });
        dag.getOutgoingEdgesOf(vertex).forEach(edgeFromMerger ->
            // Write data as arrays of bytes to prevent extra serialization.
            edgeFromMerger.setProperty(AsBytesProperty.of(AsBytesProperty.Value.WriteAsBytes)));
      }
    });
    return dag;
  }
}
