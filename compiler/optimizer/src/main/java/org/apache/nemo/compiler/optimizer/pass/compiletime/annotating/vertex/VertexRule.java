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

import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Rule;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Rule to apply on the vertices.
 */
public final class VertexRule extends Rule<IRVertex> {
  /**
   * Private constructor for the VertexRule class.
   * @param condition the condition for the rule.
   * @param action the action of the rule.
   */
  private VertexRule(final Predicate<IRVertex> condition, final Consumer<IRVertex> action) {
    super(condition, action);
  }

  /**
   * Static initializer of the VertexRule class.
   *
   * @param condition the condition of the rule.
   * @param action the action of the rule.
   * @return a new VertexRule object.
   */
  public static VertexRule of(final Predicate<IRVertex> condition, final Consumer<IRVertex> action) {
    return new VertexRule(condition, action);
  }
}
