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
package org.apache.nemo.runtime.master;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.runtime.metric.JobMetric;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link MetricStore}
 */
public final class MetricStoreTest {
  @Test
  public void testJson() throws IOException {
    final MetricStore metricStore = MetricStore.getStore();

    metricStore.getOrCreateMetric(JobMetric.class, "testId");

    final String json = metricStore.dumpMetricToJson(JobMetric.class);

    final ObjectMapper objectMapper = new ObjectMapper();
    final TreeNode treeNode = objectMapper.readTree(json);

    final TreeNode jobMetricNode = treeNode.get("JobMetric");
    assertNotNull(jobMetricNode);

    final TreeNode metricNode = jobMetricNode.get("testId");
    assertNotNull(metricNode);

    final TreeNode fieldNode = metricNode.get("id");
    assertTrue(fieldNode.isValueNode());
  }
}
