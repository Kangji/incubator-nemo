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

package org.apache.nemo.examples.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestArgs;
import org.apache.nemo.compiler.optimizer.policy.DefaultPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test Simulation.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class SimulationITCase {
  private static ArgBuilder builder;

  private static final String inputFileName = "inputs/test_input_wordcount";
  private static final String outputFileName = "test_output_wordcount";
  private static final String expectedOutputFileName = "outputs/expected_output_wordcount";
  private static final String executorResourceFileName = ExampleTestArgs.getFileBasePath() + "executors/beam_test_executor_resources.json";
  private static final String oneExecutorResourceFileName = ExampleTestArgs.getFileBasePath() + "executors/beam_test_one_executor_resources.json";
  private static final String inputFilePath = ExampleTestArgs.getFileBasePath() + inputFileName;
  private static final String outputFilePath = ExampleTestArgs.getFileBasePath() + outputFileName;

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
      .addUserMain(WordCount.class.getCanonicalName())
      .addUserArgs(inputFilePath, outputFilePath);
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testSimulationScheduler() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(WordCountITCase.class.getSimpleName() + "_simulation_scheduler")
      .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
      .addArg("parallelism", "10")
      .addArg("scheduler_impl_class_name", "org.apache.nemo.runtime.master.scheduler.SimulationScheduler")
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testSimulation() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(WordCountITCase.class.getSimpleName() + "_simulation")
      .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
      .addArg("parallelism", "10")
      .addArg("simulation", "true")
      .build());
  }
}
