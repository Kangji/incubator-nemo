/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.sailfishskew;

import edu.snu.nemo.common.coder.BytesDecoderFactory;
import edu.snu.nemo.common.coder.BytesEncoderFactory;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.*;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.SkewnessAwareSchedulingProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.SkipSerDesProperty;
import edu.snu.nemo.common.ir.vertex.transform.RelayTransform;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping.ReshapingPass;

import java.util.Collections;

/**
 * Pass to modify the DAG for a job to batch the disk seek.
 * It adds a {@link OperatorVertex} with {@link RelayTransform} before the vertices
 * receiving shuffle edges,
 * to merge the shuffled data in memory and write to the disk at once.
 */
public final class SailfishSkewRelayReshapingPass extends ReshapingPass {

  /**
   * Default constructor.
   */
  public SailfishSkewRelayReshapingPass() {
    super(Collections.singleton(DataCommunicationPatternProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    dag.topologicalDo(v -> {
      builder.addVertex(v);
      // We care about OperatorVertices that have any incoming edge that
      // has Shuffle as data communication pattern.
      if (v instanceof OperatorVertex && dag.getIncomingEdgesOf(v).stream().anyMatch(irEdge ->
              DataCommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getPropertyValue(DataCommunicationPatternProperty.class).get()))) {
        dag.getIncomingEdgesOf(v).forEach(edge -> {
          if (DataCommunicationPatternProperty.Value.Shuffle
                .equals(edge.getPropertyValue(DataCommunicationPatternProperty.class).get())) {
            // Insert a relay vertex vertex that pull data
            final OperatorVertex relayVertex = new OperatorVertex(new RelayTransform());
            //final OperatorVertex relayVertex = new OperatorVertex(new RelayDelayTransform());
            relayVertex.getExecutionProperties().put(SkipSerDesProperty.of());
            builder.addVertex(relayVertex);

            // Insert a merger vertex having transform that write received data immediately
            // before the vertex receiving shuffled data.
            final OperatorVertex iFileMergerVertex = new OperatorVertex(new RelayTransform());
            iFileMergerVertex.getExecutionProperties().put(SkipSerDesProperty.of());
            iFileMergerVertex.setProperty(SkewnessAwareSchedulingProperty.of(true));
            builder.addVertex(iFileMergerVertex);

            final IREdge newEdgeToRelay = new IREdge(DataCommunicationPatternProperty.Value.OneToOne,
                edge.getSrc(), relayVertex, edge.isSideInput());
            final IREdge newEdgeToMerger = new IREdge(DataCommunicationPatternProperty.Value.Shuffle,
                relayVertex, iFileMergerVertex, edge.isSideInput());
            edge.copyExecutionPropertiesTo(newEdgeToMerger);
            final IREdge newEdgeFromMerger = new IREdge(DataCommunicationPatternProperty.Value.OneToOne,
                iFileMergerVertex, v);

            newEdgeToRelay.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
            newEdgeToRelay.setProperty(CompressionProperty.of(CompressionProperty.Value.LZ4));
            newEdgeToRelay.setProperty(DecoderProperty.of(BytesDecoderFactory.of()));
            newEdgeToRelay.setProperty(DecompressionProperty.of(CompressionProperty.Value.None));
            newEdgeToRelay.setProperty(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.LocalFileStore));
            newEdgeToRelay.setProperty(
                PartitionerProperty.of(PartitionerProperty.Value.DataSkewHashPartitioner));
            newEdgeToRelay.setProperty(
                KeyExtractorProperty.of(edge.getPropertyValue(KeyExtractorProperty.class).get()));
            newEdgeToRelay.setProperty(MetricCollectionProperty.of(MetricCollectionProperty.Value.DataSkewRuntimePass));
            builder.connectVertices(newEdgeToRelay);

            newEdgeToMerger.setProperty(EncoderProperty.of(BytesEncoderFactory.of()));
            newEdgeToMerger.setProperty(CompressionProperty.of(CompressionProperty.Value.None));
            newEdgeToMerger.setProperty(DecoderProperty.of(BytesDecoderFactory.of()));
            newEdgeToMerger.setProperty(DecompressionProperty.of(CompressionProperty.Value.None));
            newEdgeToMerger.getExecutionProperties().remove(MetricCollectionProperty.class);
            newEdgeToMerger.setProperty(
                PartitionerProperty.of(PartitionerProperty.Value.IncrementPartitioner));
            newEdgeFromMerger.setProperty(EncoderProperty.of(BytesEncoderFactory.of()));
            newEdgeFromMerger.setProperty(CompressionProperty.of(CompressionProperty.Value.None));
            newEdgeFromMerger.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
            newEdgeFromMerger.setProperty(
                DecompressionProperty.of(CompressionProperty.Value.LZ4));
            newEdgeFromMerger.setProperty(
                PartitionerProperty.of(PartitionerProperty.Value.IncrementPartitioner));
            builder.connectVertices(newEdgeToMerger);
            builder.connectVertices(newEdgeFromMerger);
          } else {
            builder.connectVertices(edge);
          }
        });
      } else { // Others are simply added to the builder.
        dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      }
    });
    return builder.build();
  }
}
