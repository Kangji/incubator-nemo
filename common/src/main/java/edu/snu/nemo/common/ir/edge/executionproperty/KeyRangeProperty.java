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
package edu.snu.nemo.common.ir.edge.executionproperty;

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.data.KeyRange;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;

import java.util.Map;

/**
 * Key range property.
 */
public final class KeyRangeProperty extends ExecutionProperty<Map<Integer, Pair<KeyRange, Boolean>>> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private KeyRangeProperty(final Map<Integer, Pair<KeyRange, Boolean>> value) {
    super(Key.KeyRange, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static KeyRangeProperty of(final Map<Integer, Pair<KeyRange, Boolean>> value) {
    return new KeyRangeProperty(value);
  }
}
