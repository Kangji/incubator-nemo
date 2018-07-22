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

import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.executionproperty.AssociatedProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.IntermediateDataLocationAwareSchedulingProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * This policy is same as {@link MinOccupancyFirstSchedulingPolicy}, however for Tasks
 * with {@link edu.snu.nemo.common.ir.vertex.SourceVertex}, it tries to pick one of the executors
 * where the corresponding data reside.
 */
@ThreadSafe
@DriverSide
@AssociatedProperty(IntermediateDataLocationAwareSchedulingProperty.class)
public final class IntermediateDataLocationAwareSchedulingConstraint implements SchedulingConstraint {
  private static final Logger LOG = LoggerFactory.getLogger(
      IntermediateDataLocationAwareSchedulingConstraint.class.getName());
  private final BlockManagerMaster blockManagerMaster;

  @Inject
  private IntermediateDataLocationAwareSchedulingConstraint(final BlockManagerMaster blockManagerMaster) {
    this.blockManagerMaster = blockManagerMaster;
  }

  /**
   * @return the intermediate data location in {@code taskDAG}
   */
  private Optional<String> getIntermediateDataLocation(final Task task) {
    if (task.getTaskIncomingEdges().size() == 1) {
      final StageEdge physicalStageEdge = task.getTaskIncomingEdges().get(0);
      if (DataCommunicationPatternProperty.Value.OneToOne.equals(
          physicalStageEdge.getPropertyValue(DataCommunicationPatternProperty.class)
              .orElseThrow(() -> new RuntimeException("No comm pattern!")))) {
        final String blockIdToRead =
            RuntimeIdGenerator.generateBlockId(physicalStageEdge.getId(),
                RuntimeIdGenerator.getIndexFromTaskId(task.getTaskId()));
        final BlockManagerMaster.BlockLocationRequestHandler locationHandler =
            blockManagerMaster.getBlockLocationHandler(blockIdToRead);
        if (locationHandler.getLocationFuture().isDone()) {
          try {
            final String location = locationHandler.getLocationFuture().get();
            return Optional.of(location);
          } catch (final InterruptedException | ExecutionException e) {
            LOG.error("Error during getting intermediate data location!", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    final Optional<String> optionalIntermediateLoc = getIntermediateDataLocation(task);

    if (optionalIntermediateLoc.isPresent()) {
      return optionalIntermediateLoc.get().equals(executor.getExecutorId());
    } else {
      return true;
    }
  }
}
