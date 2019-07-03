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

package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.ResourceExplorationPass;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;

/**
 * XGBoost policy, extended with an exploration pass that explores new execution properties for expanding the dataset
 * of metrics to be used with the XGBoost pass.
 */
public final class XGBoostExplorationPolicy implements Policy {
  public static final PolicyBuilder BUILDER = XGBoostPolicy.BUILDER  // build on top of the XGBoost policy.
    .registerCompileTimePass(new ResourceExplorationPass());
  private final Policy policy;

  /**
   * Default constructor.
   */
  public XGBoostExplorationPolicy() {
    this.policy = BUILDER.build();
  }

  @Override
  public IRDAG runCompileTimeOptimization(final IRDAG dag, final String dagDirectory) {
    return this.policy.runCompileTimeOptimization(dag, dagDirectory);
  }

  @Override
  public IRDAG runRunTimeOptimizations(final IRDAG dag, final Message<?> message) {
    return this.policy.runRunTimeOptimizations(dag, message);
  }
}
