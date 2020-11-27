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

import java.util.HashMap;

/**
 * Map between node name and the number of parallelism which will run on the node.
 * TODO #169: Use sites (not node names) in ResourceSiteProperty
 */
public final class ResourceSiteParallelismMapProperty extends VertexExecutionProperty<HashMap<String, Integer>> {
  /**
   * Default constructor.
   *
   * @param value the map from location to the number of Task that must be executed on the node
   */
  public ResourceSiteParallelismMapProperty(final HashMap<String, Integer> value) {
    super(value);
  }

  /**
   * Static method for constructing {@link ResourceSiteParallelismMapProperty}.
   *
   * @param value the map from location to the number of Task that must be executed on the node
   * @return the execution property
   */
  public static ResourceSiteParallelismMapProperty of(final HashMap<String, Integer> value) {
    return new ResourceSiteParallelismMapProperty(value);
  }
}
