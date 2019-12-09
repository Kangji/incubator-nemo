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
 * Test TPC program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class TPCITCase {
  private static ArgBuilder builder;

  private static final String inputFilePath = "/Users/wonook/tpcds/input/sf1";
  private static final String outputFilePath = "/users/wonook/tpcds/output/sf1";
  private static final String executorResourceFileName = ExampleTestArgs.getFileBasePath() + "executors/beam_test_executor_resources.xml";

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
      .addUserMain(TPC.class.getCanonicalName())
      .addSourceParallelism(5)
      .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
      .addResourceJson(executorResourceFileName);
  }

  private static final int TIMEOUT = 3600000;  // 40 min

  // @After
  // public void tearDown() throws Exception {
  //   try {
  //     ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
  //   } finally {
  //     ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   }
  // }

   @Test(timeout = TIMEOUT, expected = Test.None.class)
   public void testQ3() throws Exception {
     JobLauncher.main(builder
       .addJobId(TPCITCase.class.getSimpleName() + "_q3")
       .addUserArgs("q3", inputFilePath, outputFilePath)
       .build());
   }

  // @Test(timeout = TIMEOUT, expected = Test.None.class)
  // public void testQ7() throws Exception {
  //   JobLauncher.main(builder
  //     .addJobId(TPCITCase.class.getSimpleName() + "_q7")
  //     .addUserArgs("q7", inputFilePath, outputFilePath)
  //     .build());
  // }

  // @Test(timeout = TIMEOUT, expected = Test.None.class)
  // public void testQ11() throws Exception {
  //   JobLauncher.main(builder
  //     .addJobId(TPCITCase.class.getSimpleName() + "_q11")
  //     .addUserArgs("q11", inputFilePath, outputFilePath)
  //     .build());
  // }

   @Test(timeout = TIMEOUT, expected = Test.None.class)
   public void testQ22() throws Exception {
     JobLauncher.main(builder
       .addJobId(TPCITCase.class.getSimpleName() + "_q22")
       .addUserArgs("q22", inputFilePath, outputFilePath)
       .build());
   }

  // @Test(timeout = TIMEOUT, expected = Test.None.class)
  // public void testQ38() throws Exception {
  //   JobLauncher.main(builder
  //     .addJobId(TPCITCase.class.getSimpleName() + "_q38")
  //     .addUserArgs("q38", inputFilePath, outputFilePath)
  //     .build());
  // }

  // @Test(timeout = TIMEOUT, expected = Test.None.class)
  // public void testQ42() throws Exception {
  //   JobLauncher.main(builder
  //     .addJobId(TPCITCase.class.getSimpleName() + "_q42")
  //     .addUserArgs("q42", inputFilePath, outputFilePath)
  //     .build());
  // }
}
