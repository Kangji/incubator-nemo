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
package edu.snu.nemo.runtime.master.scheduler;

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorSlotComplianceProperty;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;

/**
 * This policy finds executor that has free slot for a Task.
 */
public final class ExecutorSlotComplianceSchedulingPredicate implements SchedulingPredicate<ExecutorSlotComplianceProperty> {
  @VisibleForTesting
  @Inject
  public ExecutorSlotComplianceSchedulingPredicate() {
  }

  @Override
  public boolean testSchedulability(final Task task,
                                    final ExecutorSlotComplianceProperty property,
                                    final ExecutorRepresenter executor) {
    // If this task does not have to comply to slot restrictions, skip testing and return true.
    if (!property.getValue()) {
      return true;
    }
    return executor.getRunningTasks().size() < executor.getExecutorCapacity();
  }
}
