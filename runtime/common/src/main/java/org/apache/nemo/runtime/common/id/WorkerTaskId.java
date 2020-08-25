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
package org.apache.nemo.runtime.common.id;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A dynamic ID class for tasks.
 * Supports speculative execution (task cloning) and work stealing (splitting tasks).
 *
 * Hierarchy
 * 1. Stage Id (necessary)
 * 2. Data index (necessary)
 * 3. Work Stealing index (optional)
 * 4. Clone index (optional)
 *
 * Rules
 * 1. Allowed combination: 1-2(an umbrella task id), 1-2-3, 1-2-4, 1-2-3-4
 * 2. Initialization
 */
public final class WorkerTaskId implements TaskId {
  private final String stageId;
  private final int index;
  private final ManagerTaskId parentTask;
  private final int workStealingIndex;
  private final int cloneIndex;
  private final List<WorkerTaskId> clones = new ArrayList<>(); // default 0
  private final AtomicInteger cloneIndexGenerator = new AtomicInteger(0);

  /**
   * Default constructor.
   * @param stageId   stage id.
   * @param index     partition offset to read.
   */
  public WorkerTaskId(final String stageId,
                      final int index,
                      final ManagerTaskId parentTask,
                      final int workStealingIndex,
                      final int cloneIndex) {
    this.stageId = stageId;
    this.index = index;
    this.parentTask = parentTask;
    this.workStealingIndex = workStealingIndex;
    this.cloneIndex = cloneIndex;
  }

  @Override
  public String getStageId() {
    return stageId;
  }

  @Override
  public int getDataIndex() {
    return index;
  }

  public ManagerTaskId getParentTask() {
    return parentTask;
  }

  public int getWorkStealingIndex() {
    return workStealingIndex;
  }

  public int getCloneIndex() {
    return cloneIndex;
  }

  public List<WorkerTaskId> getClones() {
    return clones;
  }

  public WorkerTaskId addClone() {
    WorkerTaskId clone = new WorkerTaskId(stageId, index, parentTask, workStealingIndex,
      cloneIndexGenerator.getAndIncrement());
    clones.add(clone);
    return clone;
  }
}
