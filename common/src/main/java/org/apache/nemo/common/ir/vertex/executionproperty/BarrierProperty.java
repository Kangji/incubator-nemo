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

package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * Vertices tagged with the barrier property triggers a runtime pass prior to the execution of the vertex.
 * Also see {@link org.apache.nemo.common.ir.vertex.utility.runtimepass.SignalVertex}, which has to be refactored.
 */
public final class BarrierProperty extends VertexExecutionProperty<String> {
  /**
   * Default constructor.
   *
   * @param value the class name of the runtime pass to trigger before executing the vertex.
   */
  public BarrierProperty(final String value) {
    super(value);
  }

  /**
   * Static method for constructing {@link BarrierProperty}.
   *
   * @param value the class name of the runtime pass to trigger before executing the vertex.
   * @return the execution property.
   */
  public static BarrierProperty of(final String value) {
    return new BarrierProperty(value);
  }
}
