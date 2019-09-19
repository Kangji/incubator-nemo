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

import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A compile-time pass that annotates the IR DAG with execution properties.
 * It is ensured by the compiler that the shape of the IR DAG itself is not modified by an AnnotatingPass.
 *
 * @param <T> type specifying the IRVertex or the IREdge.
 */
public abstract class AnnotatingPass<T> extends CompileTimePass {
  private final Set<Class<? extends ExecutionProperty>> executionPropertiesToAnnotate;
  private final Set<Class<? extends ExecutionProperty>> prerequisiteExecutionProperties;
  private final ArrayList<Rule<T>> ruleSet;

  /**
   * Constructor.
   *
   * @param cls the annotating pass class.
   */
  public AnnotatingPass(final Class<? extends AnnotatingPass> cls) {
    final Annotates annotates = cls.getAnnotation(Annotates.class);
    this.executionPropertiesToAnnotate = new HashSet<>(Arrays.asList(annotates.value()));

    final Requires requires = cls.getAnnotation(Requires.class);
    this.prerequisiteExecutionProperties = requires == null
      ? new HashSet<>() : new HashSet<>(Arrays.asList(requires.value()));

    this.ruleSet = new ArrayList<>();
  }

  /**
   * Getter for the execution properties to annotate through the pass.
   *
   * @return key of execution properties to annotate through the pass.
   */
  public final Set<Class<? extends ExecutionProperty>> getExecutionPropertiesToAnnotate() {
    return executionPropertiesToAnnotate;
  }

  @Override
  public final Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties() {
    return prerequisiteExecutionProperties;
  }

  /**
   * Add rule to the rule set.
   * @param rule the rule to add.
   */
  public final void addToRuleSet(final Rule<T> rule) {
    ruleSet.add(rule);
  }

  /**
   * @return the rule set.
   */
  public final ArrayList<Rule<T>> getRuleSet() {
    return ruleSet;
  }
}
