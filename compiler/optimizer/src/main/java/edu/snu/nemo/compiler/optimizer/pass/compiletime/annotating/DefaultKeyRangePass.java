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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.data.HashRange;
import edu.snu.nemo.common.data.KeyRange;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.KeyRangeProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import static edu.snu.nemo.common.ir.executionproperty.ExecutionProperty.Key;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Assigns default key range property.
 */
public final class DefaultKeyRangePass extends AnnotatingPass {
  public DefaultKeyRangePass() {
    super(Key.KeyRange, Stream.of(Key.Parallelism, Key.StageId).collect(Collectors.toSet()));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(dst -> {
      dag.getIncomingEdgesOf(dst).forEach(edge -> {
        final IRVertex src = edge.getSrc();
        if (src.getProperty(Key.StageId) != dst.getProperty(Key.StageId)) {
          final int parallelism = dst.getProperty(Key.Parallelism);
          final List<KeyRange> keyRanges = new ArrayList<>();
          for (int i = 0; i < parallelism; i++) {
            keyRanges.add(HashRange.of(i, i + 1));
          }
          edge.setProperty(KeyRangeProperty.of(keyRanges));
        }
      });
    });
    return null;
  }
}
