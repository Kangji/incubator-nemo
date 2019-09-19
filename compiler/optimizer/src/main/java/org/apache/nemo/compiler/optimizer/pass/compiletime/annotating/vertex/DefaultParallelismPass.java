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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

import java.util.List;

/**
 * Optimization pass for tagging parallelism execution property.
 */
@Annotates(ParallelismProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class DefaultParallelismPass extends AnnotatingPass<IRVertex> {
  // we decrease the number of parallelism by this number on each shuffle boundary.
  private final int shuffleDecreaseFactor;

  /**
   * Default constructor with a default number of source parallelism, and a shuffle decreasing factor of 2.
   */
  public DefaultParallelismPass() {
    this(2);
  }

  /**
   * Default constructor.
   *
   * @param shuffleDecreaseFactor    the parallelism decrease factor for shuffle edge.
   */
  public DefaultParallelismPass(final int shuffleDecreaseFactor) {
    super(DefaultParallelismPass.class);
    this.shuffleDecreaseFactor = shuffleDecreaseFactor;
    this.addToRuleSet(VertexRule.of(// Source vertices are already allocated with source parallelism at NemoOptimizer.
      (IRVertex vertex, IRDAG dag) -> !dag.getIncomingEdgesOf(vertex).isEmpty(),
      (IRVertex vertex, IRDAG dag) -> {
        // No reason to propagate via Broadcast edges, as the data streams that will use the broadcasted data
        // as a sideInput will have their own number of parallelism
        final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
        final Integer o2oParallelism = inEdges.stream()
          .filter(edge -> CommunicationPatternProperty.Value.ONE_TO_ONE
            .equals(edge.getPropertyValue(CommunicationPatternProperty.class).orElse(null)))
          .mapToInt(edge -> edge.getSrc().getPropertyValue(ParallelismProperty.class).orElse(1))
          .max().orElse(1);
        final Integer shuffleParallelism = inEdges.stream()
          .filter(edge -> CommunicationPatternProperty.Value.SHUFFLE
            .equals(edge.getPropertyValue(CommunicationPatternProperty.class).orElse(null)))
          .mapToInt(edge -> edge.getSrc().getPropertyValue(ParallelismProperty.class).orElse(1))
          .map(i -> i / shuffleDecreaseFactor)
          .max().orElse(1);
        // We set the greater value as the parallelism.
        final Integer parallelism = o2oParallelism > shuffleParallelism ? o2oParallelism : shuffleParallelism;
        vertex.setPropertyIfPossible(ParallelismProperty.of(parallelism));
        // synchronize one-to-one edges parallelism
        recursivelySynchronizeO2OParallelism(dag, vertex, parallelism);
      }
    ));
    this.addToRuleSet(VertexRule.of(
      (IRVertex vertex, IRDAG dag) -> dag.getIncomingEdgesOf(vertex).isEmpty()
        && !vertex.getPropertyValue(ParallelismProperty.class).isPresent(),
      (IRVertex vertex, IRDAG dag) -> {
        throw new CompileTimeOptimizationException("There is a non-source vertex that doesn't have any inEdges "
          + "(excluding SideInput edges)");
      }));
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(vertex -> this.getRuleSet().forEach(rule -> {
      if (rule.getCondition().test(vertex, dag)) {
        rule.getAction().accept(vertex, dag);
      }
    }));
    return dag;
  }

  /**
   * Recursively synchronize parallelism for vertices connected by one-to-one edges.
   *
   * @param dag         the original DAG.
   * @param vertex      vertex to observe and update.
   * @param parallelism the parallelism of the most recently updated descendant.
   * @return the max value of parallelism among those observed.
   */
  static Integer recursivelySynchronizeO2OParallelism(final IRDAG dag, final IRVertex vertex,
                                                      final Integer parallelism) {
    final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
    final Integer ancestorParallelism = inEdges.stream()
      .filter(edge -> CommunicationPatternProperty.Value.ONE_TO_ONE
        .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get()))
      .map(IREdge::getSrc)
      .mapToInt(inVertex -> recursivelySynchronizeO2OParallelism(dag, inVertex, parallelism))
      .max().orElse(1);
    final Integer maxParallelism = ancestorParallelism > parallelism ? ancestorParallelism : parallelism;
    final Integer myParallelism = vertex.getPropertyValue(ParallelismProperty.class).get();

    // update the vertex with the max value.
    if (maxParallelism > myParallelism) {
      vertex.setPropertyIfPossible(ParallelismProperty.of(maxParallelism));
      return maxParallelism;
    }
    return myParallelism;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DefaultParallelismPass that = (DefaultParallelismPass) o;
    return new EqualsBuilder()
      .append(shuffleDecreaseFactor, that.shuffleDecreaseFactor)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(shuffleDecreaseFactor)
      .toHashCode();
  }
}
