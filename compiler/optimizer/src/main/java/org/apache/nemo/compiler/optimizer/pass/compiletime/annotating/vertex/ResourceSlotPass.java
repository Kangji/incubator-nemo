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

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

/**
 * Sets {@link ResourceSlotProperty}.
 */
@Annotates(ResourceSlotProperty.class)
public final class ResourceSlotPass extends AnnotatingPass<IRVertex> {

  /**
   * Constructor.
   */
  public ResourceSlotPass() {
    super(ResourceSlotPass.class);
    this.addToRuleSet(VertexRule.of("ResourceSlot",
      // On every vertex, if ResourceSlotProperty is not set, put it as true.
      (IRVertex vertex, IRDAG dag) -> true,
      (IRVertex vertex, IRDAG dag) -> vertex.setPropertyIfAbsent(ResourceSlotProperty.of(true))));
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
}
