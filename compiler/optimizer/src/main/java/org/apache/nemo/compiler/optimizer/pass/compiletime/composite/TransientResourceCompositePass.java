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
package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.edge.TransientResourceDataFlowPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.edge.TransientResourceDataStorePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.vertex.TransientResourcePriorityPass;

import java.util.Arrays;

/**
 * A series of passes to harness transient resources.
 * Ref: https://dl.acm.org/citation.cfm?id=3064181
 */
public final class TransientResourceCompositePass extends CompositePass {
  /**
   * Default constructor.
   */
  public TransientResourceCompositePass() {
    super(Arrays.asList(
      new TransientResourcePriorityPass(),
      new TransientResourceDataStorePass(),
      new TransientResourceDataFlowPass()
    ));
  }
}
