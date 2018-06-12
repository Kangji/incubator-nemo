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

import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for {@link SchedulingPolicy}.
 */
@DriverSide
@ThreadSafe
public final class SchedulingPolicyRegistry {
  private final Map<Type, SchedulingPolicy> schedulingPolicyMap = new ConcurrentHashMap<>();

  @Inject
  private SchedulingPolicyRegistry(final MinOccupancySchedulingPolicy minOccupancySchedulingPolicy) {
    registerSchedulingPolicy(minOccupancySchedulingPolicy);
  }

  /**
   * Registers a {@link SchedulingPolicy}.
   * @param policy the policy to register
   */
  public void registerSchedulingPolicy(final SchedulingPolicy policy) {
    for (final Type interfaceType : policy.getClass().getGenericInterfaces()) {
      if (!(interfaceType instanceof ParameterizedType)) {
        continue;
      }
      final ParameterizedType type = (ParameterizedType) interfaceType;
      if (!type.getRawType().equals(SchedulingPolicy.class)) {
        continue;
      }
      final Type[] typeArguments = type.getActualTypeArguments();
      if (typeArguments.length != 1) {
        throw new RuntimeException(String.format("SchedulingPolicy %s has wrong number of type parameters.",
            policy.getClass()));
      }
      final Type executionPropertyType = typeArguments[0];
      if (schedulingPolicyMap.putIfAbsent(executionPropertyType, policy) != null) {
        throw new RuntimeException(String.format("Multiple SchedulingPolicy for ExecutionProperty %s", type));
      }
    }
  }

  /**
   * Returns {@link SchedulingPolicy} for the given {@link ExecutionProperty}.
   * @param propertyClass {@link ExecutionProperty}.
   * @return {@link SchedulingPolicy} object, or {@link Optional#EMPTY}.
   */
  public Optional<SchedulingPolicy> get(final Class<? extends ExecutionProperty> propertyClass) {
    return Optional.ofNullable(schedulingPolicyMap.get(propertyClass));
  }
}
