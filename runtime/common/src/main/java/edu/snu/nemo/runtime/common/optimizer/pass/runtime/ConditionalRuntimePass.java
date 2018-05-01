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
package edu.snu.nemo.runtime.common.optimizer.pass.runtime;

import edu.snu.nemo.common.eventhandler.RuntimeEventHandler;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalPlan;

import java.util.Set;
import java.util.function.BiFunction;

/**
 * Wrapper for {@link RuntimePass} that provides conditional triggering.
 */
public final class ConditionalRuntimePass<T> implements RuntimePass<T> {
  private final BiFunction<PhysicalPlan, T, Boolean> predicate;
  private final RuntimePass<T> innerPass;

  public ConditionalRuntimePass(final BiFunction<PhysicalPlan, T, Boolean> predicate, final RuntimePass<T> innerPass) {
    this.predicate = predicate;
    this.innerPass = innerPass;
  }

  @Override
  public PhysicalPlan apply(final PhysicalPlan physicalPlan, final T metric) {
    return predicate.apply(physicalPlan, metric) ? innerPass.apply(physicalPlan, metric) : physicalPlan;
  }

  @Override
  public Set<Class<? extends RuntimeEventHandler>> getEventHandlerClasses() {
    return innerPass.getEventHandlerClasses();
  }
}
