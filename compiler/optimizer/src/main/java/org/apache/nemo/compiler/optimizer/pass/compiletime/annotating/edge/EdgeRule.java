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

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Rule;

import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

/**
 * Rule to apply on the edges.
 */
public final class EdgeRule extends Rule<IREdge> {
  /**
   * Private constructor for the EdgeRule class.
   *
   * @param name the name of the optimization rule.
   * @param condition the condition for the rule.
   * @param action the action of the rule.
   */
  private EdgeRule(final String name,
                   final BiPredicate<IREdge, IRDAG> condition, final BiConsumer<IREdge, IRDAG> action) {
    super(name, condition, action);
  }

  /**
   * Static initializer of the EdgeRule class.
   *
   * @param name the name of the optimization rule.
   * @param condition the condition of the rule.
   * @param action the action of the rule.
   * @return a new EdgeRule object.
   */
  public static EdgeRule of(final String name,
                            final BiPredicate<IREdge, IRDAG> condition, final BiConsumer<IREdge, IRDAG> action) {
    return new EdgeRule(name, condition, action);
  }
}
