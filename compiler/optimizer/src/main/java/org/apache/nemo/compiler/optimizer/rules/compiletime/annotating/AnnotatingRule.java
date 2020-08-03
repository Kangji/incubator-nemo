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

package org.apache.nemo.compiler.optimizer.rules.compiletime.annotating;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.compiler.optimizer.rules.compiletime.CompileTimeRule;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A rule for annotating execution properties. The shape itself is not modified by an annotating rule.
 * @param <T> the type of the component to modify: IRVertex or IREdge.
 */
public abstract class AnnotatingRule<T extends Serializable> extends CompileTimeRule<T> {
  private final Set<Class<? extends ExecutionProperty>> executionPropertiesToAnnotate;

  /**
   * Constructor.
   * @param cls class of the annotating rule.
   * @param conditions conditions of the annotating rule.
   */
  public AnnotatingRule(final Class<? extends AnnotatingRule> cls, final Predicate<Pair<T, IRDAG>>... conditions) {
    super(cls, conditions);

    final Annotates annotates = cls.getAnnotation(Annotates.class);
    this.executionPropertiesToAnnotate = new HashSet<>(Arrays.asList(annotates.value()));
  }

  /**
   * Getter for the execution properties to annotate through the rule.
   *
   * @return key of execution properties to annotate through the rule.
   */
  public final Set<Class<? extends ExecutionProperty>> getExecutionPropertiesToAnnotate() {
    return executionPropertiesToAnnotate;
  }

  /**
   * Apply the rule and annotate the execution propety.
   */
  public abstract void applyRuleAndAnnotate(final T componentToOptimize);
}
