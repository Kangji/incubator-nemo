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
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A policy that randomly runs a random policy for exploration about the performances in different settings.
 */
public final class ExplorationPolicy implements Policy {
  private static final Logger LOG = LoggerFactory.getLogger(ExplorationPolicy.class.getName());

  private final Policy policy;
  private static final List<Policy> CANDIDATES = new ArrayList<>();

  /**
   * Default constructor.
   */
  public ExplorationPolicy() {
    CANDIDATES.add(new BasicPullPolicy());
    CANDIDATES.add(new BasicPushPolicy());
    CANDIDATES.add(new ConditionalLargeShufflePolicy());
    CANDIDATES.add(new DataSkewPolicy());
    CANDIDATES.add(new DefaultPolicy());
    CANDIDATES.add(new DisaggregationPolicy());
    CANDIDATES.add(new LargeShufflePolicy());
    CANDIDATES.add(new SamplingLargeShuffleSkewPolicy());
    CANDIDATES.add(new TransientResourcePolicy());
    Collections.shuffle(CANDIDATES);
    this.policy = CANDIDATES.get(0);
    LOG.info("Running " + this.policy.getClass().getSimpleName()
      + " for the " + ExplorationPolicy.class.getSimpleName());
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
