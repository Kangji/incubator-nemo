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
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.compiler.optimizer.policy.*;
import org.apache.nemo.examples.beam.policy.AggressiveSpeculativeCloningPolicy;
import org.apache.nemo.examples.beam.policy.UpfrontSchedulingPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test WordCount program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class WordCountITCase {
  private static ArgBuilder builder;

  private static final String inputFileName = "inputs/test_input_wordcount";
  private static final String outputFileName = "test_output_wordcount";
  private static final String expectedOutputFileName = "outputs/expected_output_wordcount";
  private static final String executorResourceFileName = ExampleTestArgs.getFileBasePath() + "executors/beam_test_executor_resources.xml";
  private static final String oneExecutorResourceFileName = ExampleTestArgs.getFileBasePath() + "executors/beam_test_one_executor_resources.xml";
  private static final String inputFilePath = ExampleTestArgs.getFileBasePath() + inputFileName;
  private static final String outputFilePath = ExampleTestArgs.getFileBasePath() + outputFileName;

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
      .addUserMain(WordCount.class.getCanonicalName())
      .addUserArgs(inputFilePath, outputFilePath);
  }

  @After
  public void tearDown() throws Exception {
    try {
      ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    } finally {
      ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void test() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(WordCountITCase.class.getSimpleName())
      .addSourceParallelism(5)
      .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testDBEnabled() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addEnableDB()
      .addJobId(WordCountITCase.class.getSimpleName() + "_dbEnabled")
      .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testXGBoost() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addEnableDB()
      .addJobId(WordCountITCase.class.getSimpleName() + "_xgboost")
      .addOptimizationPolicy(XGBoostPolicy.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testLargeShuffle() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(WordCountITCase.class.getSimpleName() + "_largeShuffle")
      .addSourceParallelism(5)
      .addOptimizationPolicy(LargeShufflePolicy.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testLargeShuffleInOneExecutor() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(oneExecutorResourceFileName)
      .addJobId(WordCountITCase.class.getSimpleName() + "_largeshuffleInOneExecutor")
      .addSourceParallelism(5)
      .addOptimizationPolicy(LargeShufflePolicy.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testConditionalLargeShuffle() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(WordCountITCase.class.getSimpleName() + "_conditionalLargeShuffle")
      .addOptimizationPolicy(ConditionalLargeShufflePolicy.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testTransientResource() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(WordCountITCase.class.getSimpleName() + "_transient")
      .addSourceParallelism(5)
      .addOptimizationPolicy(TransientResourcePolicy.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testClonedScheduling() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(WordCountITCase.class.getSimpleName() + "_clonedscheduling")
      .addMaxTaskAttempt(Integer.MAX_VALUE)
      .addSourceParallelism(5)
      .addOptimizationPolicy(UpfrontSchedulingPolicy.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testSpeculativeExecution() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(WordCountITCase.class.getSimpleName() + "_speculative")
      .addMaxTaskAttempt(Integer.MAX_VALUE)
      .addSourceParallelism(5)
      .addOptimizationPolicy(AggressiveSpeculativeCloningPolicy.class.getCanonicalName())
      .build());
  }
}
