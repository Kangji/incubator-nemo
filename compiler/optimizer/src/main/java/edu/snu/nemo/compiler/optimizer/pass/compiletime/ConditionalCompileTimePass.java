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
package edu.snu.nemo.compiler.optimizer.pass.compiletime;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.util.Set;
import java.util.function.Predicate;

/**
 * Wrapper for {@link CompileTimePass} that provides conditional triggering.
 */
public final class ConditionalCompileTimePass implements CompileTimePass {
  private final Predicate<DAG<IRVertex, IREdge>> predicate;
  private final CompileTimePass innerPass;

  public ConditionalCompileTimePass(final Predicate<DAG<IRVertex, IREdge>> predicate, final CompileTimePass innerPass) {
    this.predicate = predicate;
    this.innerPass = innerPass;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> irDag) {
    return predicate.test(irDag) ? innerPass.apply(irDag) : irDag;
  }

  @Override
  public Set<ExecutionProperty.Key> getPrerequisiteExecutionProperties() {
    return innerPass.getPrerequisiteExecutionProperties();
  }
}
