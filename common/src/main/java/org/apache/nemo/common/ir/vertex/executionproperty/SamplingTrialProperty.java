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

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * key: total number of sampling trials.
 * value: number of this trial
 */
public class SamplingTrialProperty extends VertexExecutionProperty<Pair<Integer, Integer>> {

  public SamplingTrialProperty(final Integer key, final Integer value) {
    super(Pair.of(key, value));
  }

  public static SamplingTrialProperty of(final Integer key, final Integer value) {
    return new SamplingTrialProperty(key, value);
  }
}
