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

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Rule class.
 * @param <T> the IREdge or the IRVertex.
 */
public abstract class Rule<T> implements Serializable {
  private final Predicate<T> condition;
  private final Consumer<T> action;

  /**
   * Private constructor for the EdgeRule class.
   * @param condition the condition for the rule.
   * @param action the action of the rule.
   */
  public Rule(final Predicate<T> condition, final Consumer<T> action) {
    this.condition = condition;
    this.action = action;
  }

  /**
   * @return the condition of the rule.
   */
  public Predicate<T> getCondition() {
    return condition;
  }

  /**
   * @return the action of the rule.
   */
  public Consumer<T> getAction() {
    return action;
  }
}
