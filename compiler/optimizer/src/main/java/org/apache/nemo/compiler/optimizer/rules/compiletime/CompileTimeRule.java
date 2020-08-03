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

package org.apache.nemo.compiler.optimizer.rules.compiletime;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Abstract class for optimization rules run during the compile-time.
 * @param <T> the targeting type: either IRVertex or IREdge.
 */
public abstract class CompileTimeRule<T extends Serializable> {
  private Predicate<Pair<T, IRDAG>> condition;
  private final Set<Class<? extends ExecutionProperty>> prerequisiteExecutionProperties;

  /**
   * Default constructor.
   * @param conditions predicate conditions for the rule, each for an IRVertex/IREdge and a IRDAG pair.
   */
  public CompileTimeRule(final Class<? extends CompileTimeRule> cls, final Predicate<Pair<T, IRDAG>>... conditions) {
    for (final Predicate<Pair<T, IRDAG>> cond: conditions) {
      this.condition = this.condition.and(cond);
    }

    final Requires requires = cls.getAnnotation(Requires.class);
    this.prerequisiteExecutionProperties = requires == null
      ? new HashSet<>() : new HashSet<>(Arrays.asList(requires.value()));
  }

  /**
   * Getter for prerequisite execution properties.
   *
   * @return set of prerequisite execution properties.
   */
  public final Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties() {
    return this.prerequisiteExecutionProperties;
  }
}
