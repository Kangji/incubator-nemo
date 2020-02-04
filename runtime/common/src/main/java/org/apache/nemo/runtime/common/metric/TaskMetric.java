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
package org.apache.nemo.runtime.common.metric;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.runtime.common.state.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric class for {@link org.apache.nemo.runtime.common.plan.Task}.
 */
public class TaskMetric implements StateMetric<TaskState.State> {
  private String id;
  private String taskContainerId = "";
  private int taskScheduleAttempt = -1;
  private List<StateTransitionEvent<TaskState.State>> stateTransitionEvents = new ArrayList<>();
  private long taskDurationTime;
  private long taskCPUTime;
  private long taskSerializationTime;
  private long taskDeserializationTime = -1;
  private long taskBoundedSourceReadTime = -1;
  private long taskInputBytes = -1;
  private long taskOutputBytes = -1;
  private long taskSerializedReadBytes = -1;
  private long taskEncodedReadBytes = -1;
  private long taskPeakExecutionMemory = -1;
  private long taskShuffleReadBytes = -1;
  private long taskShuffleReadTime = -1;
  private long taskShuffleWriteBytes = -1;
  private long taskShuffleWriteTime = -1;

  private static final Logger LOG = LoggerFactory.getLogger(TaskMetric.class.getName());

  public TaskMetric(final String id) {
    this.id = id;
  }

  @Override
  public final String getId() {
    return id;
  }

  public final String getTaskContainerId() {
    return taskContainerId;
  }

  private void setTaskContainerId(final String containerId) {
    this.taskContainerId = containerId;
  }

  public final int getTaskScheduleAttempt() {
    return taskScheduleAttempt;
  }

  private void setTaskScheduleAttempt(final int scheduleAttempt) {
    this.taskScheduleAttempt = scheduleAttempt;
  }

  @Override
  public final List<StateTransitionEvent<TaskState.State>> getStateTransitionEvents() {
    return stateTransitionEvents;
  }

  @Override
  public final void addEvent(final TaskState.State prevState, final TaskState.State newState) {
    stateTransitionEvents.add(new StateTransitionEvent<>(System.currentTimeMillis(), prevState, newState));
  }

  private void addEvent(final StateTransitionEvent<TaskState.State> event) {
    stateTransitionEvents.add(event);
  }

  public final long getTaskDurationTime(){
    return taskDurationTime;
  }

  private void setTaskDurationTime(final long taskDurationTime) {
    this.taskDurationTime = taskDurationTime;
  }

  public final long getTaskCPUTime() {
    return taskCPUTime;
  }

  private void setTaskCPUTime(final long taskCPUTime) {
    this.taskCPUTime = taskCPUTime;
  }

  public final long getTaskSerializationTime() {
    return taskSerializationTime;
  }

  public void setTaskSerializationTime(final long taskSerializationTime) {
    this.taskSerializationTime = taskSerializationTime;
  }

  public final long getTaskDeserializationTime() {
    return taskDeserializationTime;
  }

  private void setTaskDeserializationTime(final long taskDeserializationTime) {
    this.taskDeserializationTime = taskDeserializationTime;
  }

  public final long getTaskBoundedSourceReadTime() {
    return taskBoundedSourceReadTime;
  }

  private void setTaskBoundedSourceReadTime(final long boundedSourceReadTime) {
    this.taskBoundedSourceReadTime = boundedSourceReadTime;
  }

  public final long getTaskOutputBytes() {
    return taskOutputBytes;
  }

  private void setTaskOutputBytes(final long taskOutputBytes) {
    this.taskOutputBytes = taskOutputBytes;
  }

  public final long getTaskSerializedReadBytes() {
    return taskSerializedReadBytes;
  }

  private void setTaskSerializedReadBytes(final long serializedReadBytes) {
    this.taskSerializedReadBytes = serializedReadBytes;
  }

  public final long getTaskEncodedReadBytes() {
    return taskEncodedReadBytes;
  }

  private void setTaskEncodedReadBytes(final long encodedReadBytes) {
    this.taskEncodedReadBytes = encodedReadBytes;
  }

  public final long getTaskPeakExecutionMemory() {
    return taskPeakExecutionMemory;
  }

  private void setTaskPeakExecutionMemory(final long taskPeakExecutionMemory) {
    this.taskPeakExecutionMemory = taskPeakExecutionMemory;
  }

  public final long getTaskShuffleReadBytes() {
    return taskShuffleReadBytes;
}

  private void setTaskShuffleReadBytes(final long taskShuffleReadBytes) {
    this.taskShuffleReadBytes = taskShuffleReadBytes;
  }

  public final long getTaskShuffleReadTime() {
    return taskShuffleReadTime;
  }

  private void setTaskShuffleReadTime(final long taskShuffleReadTime) {
    this.taskShuffleReadTime = taskShuffleReadTime;
  }

  public final long getTaskShuffleWriteBytes() {
    return taskShuffleWriteBytes;
  }

  private void setTaskShuffleWriteBytes(final long taskShuffleWriteBytes) {
    this.taskShuffleWriteBytes = taskShuffleWriteBytes;
  }

  public final long getTaskShuffleWriteTime() {
    return this.taskShuffleWriteTime;
  }

  private void setTaskShuffleWriteTime(final long taskShuffleWriteTime) {
    this.taskShuffleWriteTime = taskShuffleWriteTime;
  }

  @Override
  public boolean processMetricMessage(final String metricField, final byte[] metricValue) {
    final TaskMetrics metricFieldEnum = TaskMetrics.valueOf(metricField);
    LOG.debug("metric {} is just arrived!", metricFieldEnum);
    switch (metricFieldEnum) {
      case TASK_CONTAINER_ID:
        setTaskContainerId(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_SCHEDULE_ATTEMPT:
        setTaskScheduleAttempt(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_STATE_TRANSITION_EVENT:
        final StateTransitionEvent<TaskState.State> newStateTransitionEvent =
          SerializationUtils.deserialize(metricValue);
        addEvent(newStateTransitionEvent);
        break;
      case TASK_DURATION_TIME:
        setTaskDurationTime(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_CPU_TIME:
        setTaskCPUTime(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_SERIALIZATION_TIME:
        setTaskSerializationTime(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_DESERIALIZATION_TIME:
        setTaskDeserializationTime(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_BOUNDED_SOURCE_READ_TIME:
        setTaskBoundedSourceReadTime(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_OUTPUT_BYTES:
        setTaskOutputBytes(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_SERIALIZED_READ_BYTES:
        setTaskSerializedReadBytes(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_ENCODED_READ_BYTES:
        setTaskEncodedReadBytes(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_PEAK_EXECUTION_MEMORY:
        setTaskPeakExecutionMemory(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_SHUFFLE_READ_BYTES:
        setTaskShuffleReadBytes(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_SHUFFLE_READ_TIME:
        setTaskShuffleReadTime(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_SHUFFLE_WRITE_BYTES:
        setTaskShuffleWriteBytes(SerializationUtils.deserialize(metricValue));
        break;
      case TASK_SHUFFLE_WRITE_TIME:
        setTaskShuffleWriteTime(SerializationUtils.deserialize(metricValue));
        break;
      default:
        LOG.warn("metricField {} is not supported.", metricField);
        return false;
    }
    return true;
  }

  public enum TaskMetrics{
    TASK_CONTAINER_ID,
    TASK_SCHEDULE_ATTEMPT,
    TASK_STATE_TRANSITION_EVENT,

    TASK_DURATION_TIME, //done
    TASK_CPU_TIME, // done

    TASK_SERIALIZATION_TIME,
    TASK_DESERIALIZATION_TIME,
    TASK_BOUNDED_SOURCE_READ_TIME,

    TASK_OUTPUT_BYTES,
    TASK_SERIALIZED_READ_BYTES,
    TASK_ENCODED_READ_BYTES,

    TASK_PEAK_EXECUTION_MEMORY,

    TASK_SHUFFLE_READ_BYTES,
    TASK_SHUFFLE_READ_TIME,
    TASK_SHUFFLE_WRITE_BYTES,
    TASK_SHUFFLE_WRITE_TIME,
  }
}
