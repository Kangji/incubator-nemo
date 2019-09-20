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
import org.apache.nemo.common.ir.edge.executionproperty.DataPersistenceProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

/**
 * Pass for initiating IREdge data persistence ExecutionProperty with default values.
 */
@Annotates(DataPersistenceProperty.class)
@Requires(DataStoreProperty.class)
public final class DefaultDataPersistencePass extends AnnotatingPass<IREdge> {

  /**
   * Default constructor.
   */
  public DefaultDataPersistencePass() {
    super(DefaultDataPersistencePass.class);
    this.addToRuleSet(EdgeRule.of("DefaultDataPersistenceDiscard",
      (IREdge edge, IRDAG dag)  -> {
        final DataStoreProperty.Value dataStoreValue = edge.getPropertyValue(DataStoreProperty.class).orElse(null);
        return DataStoreProperty.Value.MEMORY_STORE.equals(dataStoreValue)
          || DataStoreProperty.Value.SERIALIZED_MEMORY_STORE.equals(dataStoreValue);
      },
      (IREdge edge, IRDAG dag)  ->
        edge.setPropertyIfAbsent(DataPersistenceProperty.of(DataPersistenceProperty.Value.DISCARD))));
    this.addToRuleSet(EdgeRule.of("DefaultDataPersistenceKeep",
      (IREdge edge, IRDAG dag)  -> {
        final DataStoreProperty.Value dataStoreValue = edge.getPropertyValue(DataStoreProperty.class).orElse(null);
        return !DataStoreProperty.Value.MEMORY_STORE.equals(dataStoreValue)
          && !DataStoreProperty.Value.SERIALIZED_MEMORY_STORE.equals(dataStoreValue);
      },
      (IREdge edge, IRDAG dag)  ->
        edge.setPropertyIfAbsent(DataPersistenceProperty.of(DataPersistenceProperty.Value.KEEP))));
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
