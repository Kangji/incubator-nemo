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

import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;

/**
 * AsBytes ExecutionProperty.
 */
public final class AsBytesProperty extends ExecutionProperty<AsBytesProperty.Value> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private AsBytesProperty(final Value value) {
    super(Key.AsBytes, value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static AsBytesProperty of(final Value value) {
    return new AsBytesProperty(value);
  }

  /**
   * Possible values of AsBytes ExecutionProperty.
   * When an edge is annotated as Write(or Read)AsBytes,
   * the writing (reading) Task writes (reads) data as arrays of bytes, instead of (de)serialized form.
   * If a data read as bytes, each element will be a serialized bytes of a partition.
   * Because of this, each element should be written to a partition respectively,
   * through partitioner like increment partitioner. (During this, the key of each partition can be changed.)
   * This bytes should be written as bytes also to restore it's contents.
   */
  public enum Value {
    ReadAsBytes,
    WriteAsBytes,
    ReadWriteAsBytes
  }
}
