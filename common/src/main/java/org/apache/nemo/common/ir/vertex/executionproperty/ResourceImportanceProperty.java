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
 * Resource property to specify the most important resource type for a vertex.
 */
public final class ResourceImportanceProperty extends VertexExecutionProperty<String> {
  /**
   * Constructor.
   *
   * @param importantResourceType the most important resource type for the vertex.
   */
  private ResourceImportanceProperty(final String importantResourceType) {
    super(importantResourceType);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param importantResourceType value of the new execution property.
   * @return the newly created execution property.
   */
  public static ResourceImportanceProperty of(final String importantResourceType) {
    return new ResourceImportanceProperty(importantResourceType);
  }

  // List of default pre-configured values.
  public static final String NONE = "NONE";
  public static final String CPU = "CPU";
  public static final String MEMORY = "MEMORY";
}
