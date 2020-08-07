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

import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.compiler.optimizer.prophet.StaticParallelismProphet;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.Map;

/**
 * Optimization pass for tagging the parallelism execution property in a smart way.
 */
@Annotates(ParallelismProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class SmartParallelismPass extends AnnotatingPass {
  /**
   * The Parallelism Prophet which tells you the optimal parallelisms for each vertex.
   */
  private final StaticParallelismProphet prophet;

  /**
   * Default constructor.
   */
  public SmartParallelismPass() {
    super(SmartParallelismPass.class);
    try {
      this.prophet = Tang.Factory.getTang().newInjector().getInstance(StaticParallelismProphet.class);
    } catch (final InjectionException e) {
      throw new CompileTimeOptimizationException(e);
    }
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    this.prophet.setCurrentIRDAG(dag);
    final Map<String, Integer> calculationResult = this.prophet.calculate();
    dag.topologicalDo(vertex -> {
      if (calculationResult.containsKey(vertex.getId())) {
        final Integer parallelism = calculationResult.get(vertex.getId());
        vertex.setProperty(ParallelismProperty.of(parallelism));
        DefaultParallelismPass.recursivelySynchronizeO2OParallelism(dag, vertex, parallelism);
      }
    });
    return dag;
  }
}
