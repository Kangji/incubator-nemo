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

import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

/**
 * Pass for initiating IREdge Encoder ExecutionProperty with default dummy coder.
 */
@Annotates(EncoderProperty.class)
public final class DefaultEdgeEncoderPass extends AnnotatingPass<IREdge> {

  private static final EncoderProperty DEFAULT_ENCODER_PROPERTY =
    EncoderProperty.of(EncoderFactory.DUMMY_ENCODER_FACTORY);

  /**
   * Default constructor.
   */
  public DefaultEdgeEncoderPass() {
    super(DefaultEdgeEncoderPass.class);
    this.addToRuleSet(EdgeRule.of(
      (IREdge edge, IRDAG dag)  -> true,
      (IREdge edge, IRDAG dag)  -> edge.setPropertyIfAbsent(DEFAULT_ENCODER_PROPERTY)));
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
}
