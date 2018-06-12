/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.common.ir.vertex.executionproperty;

import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;

/**
 * Selects MinOccupancySchedulingPolicy.
 */
public final class MinOccupancySchedulingPolicyProperty extends ExecutionProperty<Void> {

  private static final MinOccupancySchedulingPolicyProperty PROPERTY = new MinOccupancySchedulingPolicyProperty();

  /**
   * Default constructor.
   */
  private MinOccupancySchedulingPolicyProperty() {
    super(Key.MinOccupancySchedulingPolicy, null);
  }

  /**
   * Static method exposing the constructor.
   * @return the newly created execution property.
   */
  public static MinOccupancySchedulingPolicyProperty of() {
    return PROPERTY;
  }
}
