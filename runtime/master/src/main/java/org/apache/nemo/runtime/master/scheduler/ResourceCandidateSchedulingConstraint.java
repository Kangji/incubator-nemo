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

package org.apache.nemo.runtime.master.scheduler;

import net.jcip.annotations.ThreadSafe;
import org.apache.nemo.common.ir.executionproperty.AssociatedProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceCandidateProperty;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Optional;

/**
 * Check if the executor is listed up as a candidate on the resource candidate property.
 */
@ThreadSafe
@DriverSide
@AssociatedProperty(ResourceCandidateProperty.class)
public final class ResourceCandidateSchedulingConstraint implements SchedulingConstraint {
  /**
   * Default constructor.
   */
  @Inject
  ResourceCandidateSchedulingConstraint() {
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    final Optional<ArrayList<String>> executorCandidates = task.getPropertyValue(ResourceCandidateProperty.class);
    return executorCandidates.isPresent() && !executorCandidates.get().isEmpty()
      && executorCandidates.get().contains(executor.getNodeName());
  }
}
