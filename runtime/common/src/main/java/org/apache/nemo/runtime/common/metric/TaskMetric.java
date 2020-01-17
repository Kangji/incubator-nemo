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
import java.util.HashMap;
import java.util.List;

/**
 * Metric class for {@link org.apache.nemo.runtime.common.plan.Task}.
 *
 * set methods are used in
 */
public class TaskMetric implements StateMetric<TaskState.State> {
  private final int DEFAULT_VALUE = -1;

  private String id;
  private List<StateTransitionEvent<TaskState.State>> stateTransitionEvents = new ArrayList<>();
  private long serializedReadBytes = DEFAULT_VALUE;
  private long encodedReadBytes = DEFAULT_VALUE;
  private long writtenBytes = DEFAULT_VALUE;
  private long boundedSourceReadTime = DEFAULT_VALUE;
  private long taskDeserializationTime = DEFAULT_VALUE;
  private int scheduleAttempt = DEFAULT_VALUE;
  private String containerId = "";

  private static final Logger LOG = LoggerFactory.getLogger(TaskMetric.class.getName());

  public TaskMetric(final String id) {
    this.id = id;
  }

  public final long getSerializedReadBytes() {
    return serializedReadBytes;
  }

  public void setSerializedReadBytes(final long serializedReadBytes) {
    this.serializedReadBytes = serializedReadBytes;
  }

  public final long getEncodedReadBytes() {
    return encodedReadBytes;
  }

  public void setEncodedReadBytes(final long encodedReadBytes) {
    this.encodedReadBytes = encodedReadBytes;
  }

  public final long getBoundedSourceReadTime() {
    return boundedSourceReadTime;
  }

  public void setBoundedSourceReadTime(final long boundedSourceReadTime) {
    this.boundedSourceReadTime = boundedSourceReadTime;
  }

  public final long getTaskDeserializationTime() {
    return taskDeserializationTime;
  }

  public void setTaskDeserializationTime(final long taskDeserializationTime) {
    this.taskDeserializationTime = taskDeserializationTime;
  }

  public final long getWrittenBytes() {
    return writtenBytes;
  }

  public void setWrittenBytes(final long writtenBytes) {
    this.writtenBytes = writtenBytes;
  }

  public final int getScheduleAttempt() {
    return scheduleAttempt;
  }

  public void setScheduleAttempt(final int scheduleAttempt) {
    this.scheduleAttempt = scheduleAttempt;
  }

  public final String getContainerId() {
    return containerId;
  }

  public void setContainerId(final String containerId) {
    this.containerId = containerId;
  }

  @Override
  public final List<StateTransitionEvent<TaskState.State>> getStateTransitionEvents() {
    return stateTransitionEvents;
  }

  @Override
  public final String getId() {
    return id;
  }

  @Override
  public final void addEvent(final TaskState.State prevState, final TaskState.State newState) {
    stateTransitionEvents.add(new StateTransitionEvent<>(System.currentTimeMillis(), prevState, newState));
  }

  private void addEvent(final StateTransitionEvent<TaskState.State> event) {
    stateTransitionEvents.add(event);
  }

  public enum TaskMetricField {
    SERIALIZED_READ_BYTES,
    ENCODED_READ_BYTES,
    WRITTEN_BYTES,
    BOUNDED_SOURCE_READ_TIME,
    TASK_DESERIALIZATION_TIME,

    /* newly added from here */
    TASK_DURATION_TIME,
    TASK_CPU_TIME,
    TASK_SERIALIZATION_TIME,
    TASK_PEAK_EXECUTION_MEMORY,
    TASK_INPUT_BYTES,
    TASK_OUTPUT_BYTES, //maybe same concept as written bytes
    TASK_SHUFFLE_READ_BYTES,
    TASK_SHUFFLE_READ_TIME,
    TASK_SHUFFLE_WRITE_BYTES,
    TASK_SHUFFLE_WRITE_TIME,
  }

  public HashMap<TaskMetricField, Long> stackMetrics(){
    HashMap<TaskMetricField, Long> metrics = new HashMap<>();

    return metrics;
  }

  @Override
  // this is for the metricStore in apache.nemo.runtime.master
  public final boolean processMetricMessage(final String metricField, final byte[] metricValue) {
    LOG.debug("metric {} is just arrived!", metricField);

    switch (metricField) {
      case "containerId":
        setContainerId(SerializationUtils.deserialize(metricValue));
        break;
      case "scheduleAttempt":
        setScheduleAttempt(SerializationUtils.deserialize(metricValue));
        break;
      case "stateTransitionEvent":
        final StateTransitionEvent<TaskState.State> newStateTransitionEvent =
          SerializationUtils.deserialize(metricValue);
        addEvent(newStateTransitionEvent);
        break;
      case "metricList":
        if (processMetricMessageList(metricValue)) {
          break;
        }
      default:
        LOG.warn("metricField {} is not supported.", metricField);
        return false;
    }
    return true;
  }

  public final boolean processMetricMessageList(final byte[] metricValue) {
    HashMap<TaskMetricField, Long> metricList = SerializationUtils.deserialize(metricValue);
    for(TaskMetricField metricField : metricList.keySet()){
      switch (metricField) {
        case SERIALIZED_READ_BYTES:
          setSerializedReadBytes(metricList.get(metricField));
          break;
        case ENCODED_READ_BYTES:
          setEncodedReadBytes(metricList.get(metricField));
          break;
        case BOUNDED_SOURCE_READ_TIME:
          setBoundedSourceReadTime(metricList.get(metricField));
          break;
        case WRITTEN_BYTES:
          setWrittenBytes(metricList.get(metricField));
          break;
        case TASK_DESERIALIZATION_TIME:
          setTaskDeserializationTime(metricList.get(metricField));
          break;
        case TASK_DURATION_TIME:
        case TASK_CPU_TIME:
        case TASK_SERIALIZATION_TIME:
        case TASK_PEAK_EXECUTION_MEMORY:
        case TASK_INPUT_BYTES:
        case TASK_OUTPUT_BYTES:
        case TASK_SHUFFLE_READ_BYTES:
        case TASK_SHUFFLE_READ_TIME:
        case TASK_SHUFFLE_WRITE_BYTES:
        case TASK_SHUFFLE_WRITE_TIME:
        default:
          LOG.warn("metricField {} is not supported.", metricField);
          return false;
      }
    }
    return false;
  }
}
