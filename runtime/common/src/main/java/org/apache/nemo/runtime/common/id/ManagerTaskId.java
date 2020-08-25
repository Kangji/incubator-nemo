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
 * Number of Manager Tasks per stage equals the number of parallelism.
 */
public final class ManagerTaskId implements TaskId {
  private final String stageId;
  private final int index;
  private final AtomicInteger workStealingTasksIndexGenerator = new AtomicInteger(0);
  private final List<WorkerTaskId> workStealingTaskList = new ArrayList<>(); // default 1

  /**
   * Default constructor.
   * @param stageId stage id.
   * @param index   data index.
   */
  ManagerTaskId(final String stageId, final int index) {
    this.stageId = stageId;
    this.index = index;
    workStealingTaskList.add(new WorkerTaskId(stageId, index, this,
      workStealingTasksIndexGenerator.getAndIncrement(), 0));
  }

  @Override
  public String getStageId() {
    return stageId;
  }

  @Override
  public int getDataIndex() {
    return index;
  }

  public boolean generateWorkStealingTasks() {
    if (!workStealingTaskList.get(0).getClones().isEmpty()) { // clone created : cannot split tasks
      return false;
    }
    WorkerTaskId workStealingTask = new WorkerTaskId(stageId, index, this,
      workStealingTasksIndexGenerator.getAndIncrement(), 0);
    workStealingTaskList.add(workStealingTask);

    return true;
  }

  public WorkerTaskId generateCloneTask(final int workerTaskIdx) {
    WorkerTaskId workerToClone = workStealingTaskList.get(workerTaskIdx);
    return workerToClone.addClone();
  }

  public List<WorkerTaskId> getWorkStealingTaskList() {
    return workStealingTaskList;
  }

  public List<WorkerTaskId> getAllClones() {
    List<WorkerTaskId> clones = new ArrayList<>();
    for (WorkerTaskId wsTask : workStealingTaskList) {
      clones.addAll(wsTask.getClones());
    }
    return clones;
  }

  public boolean isWorkerIncluded(final WorkerTaskId workerTaskId) {
    if (workerTaskId.getWorkStealingIndex() >= workStealingTaskList.size()) {
      return false;
    }
    final WorkerTaskId wsTaskId = workStealingTaskList.get(workerTaskId.getWorkStealingIndex());
    return workerTaskId.getCloneIndex() <= wsTaskId.getClones().size();
  }
}
