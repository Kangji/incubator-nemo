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

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

import java.util.HashSet;

/**
 * List of pair of set of node names and required number of executors
 * to limit the scheduling of the tasks of the vertex to while shuffling.
 */
public final class ShuffleExecutorSetProperty
  extends VertexExecutionProperty<HashSet<Pair<HashSet<String>, Pair<MutableInt, Integer>>>> {

  /**
   * Default constructor.
   * @param value value of the execution property.
   */
  private ShuffleExecutorSetProperty(final HashSet<Pair<HashSet<String>, Pair<MutableInt, Integer>>> value) {
    super(value);
  }

  /**
   * Static method for constructing {@link ShuffleExecutorSetProperty}.
   *
   * @param value the list of pair of executors and required number to schedule the tasks of the vertex on.
   *                        Leave empty to make it effectless.
   * @return the new execution property
   */
  public static ShuffleExecutorSetProperty of(final HashSet<Pair<HashSet<String>, Pair<MutableInt, Integer>>> value) {
    return new ShuffleExecutorSetProperty(value);
  }
}
