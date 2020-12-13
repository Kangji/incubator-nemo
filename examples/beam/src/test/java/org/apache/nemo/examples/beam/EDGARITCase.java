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
import org.apache.nemo.compiler.optimizer.policy.StreamingPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test EDGAR beam applications with the JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class EDGARITCase {
  private static ArgBuilder builder;

  private static final String inputFileName = "inputs/test_input_edgar";
  private static final String outputFileName = "test_output_edgar";
  private static final String expectedOutputFileName = "outputs/expected_output_edgar";
  private static final String expectedSlidingWindowOutputFileName = "outputs/expected_output_sliding_edgar";
  private static final String executorResourceFileName = ExampleTestArgs.getFileBasePath() + "executors/beam_test_executor_resources.json";
  private static final String inputFilePath = ExampleTestArgs.getFileBasePath() + inputFileName;
  private static final String outputFilePath = ExampleTestArgs.getFileBasePath() + outputFileName;

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testEDGARAvgDocSizeFixed() throws Exception {
    builder = new ArgBuilder()
      .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
      .addUserMain(EDGARAvgDocSize.class.getCanonicalName())
      .addUserArgs(inputFilePath, "fixed", outputFilePath);
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARAvgDocSizeFixed")
      .addOptimizationPolicy(StreamingPolicy.class.getCanonicalName())
      .build());

    // try {
    //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    // } finally {
    ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    // }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testEDGARAvgDocSizeSliding() throws Exception {
    builder = new ArgBuilder()
      .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
      .addUserMain(EDGARAvgDocSize.class.getCanonicalName())
      .addUserArgs(inputFilePath, "sliding", outputFilePath);

    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARAvgDocSizeSliding")
      .addOptimizationPolicy(StreamingPolicy.class.getCanonicalName())
      .build());

    // try {
    //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedSlidingWindowOutputFileName);
    // } finally {
    ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    // }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testEDGARDocumentSuccessRateFixed() throws Exception {
    builder = new ArgBuilder()
      .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
      .addUserMain(EDGARDocumentSuccessRate.class.getCanonicalName())
      .addUserArgs(inputFilePath, "fixed", outputFilePath);

    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARDocumentSuccessRateFixed")
      .addOptimizationPolicy(StreamingPolicy.class.getCanonicalName())
      .build());

    // try {
    //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    // } finally {
    ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    // }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testEDGARDocumentSuccessRateSliding() throws Exception {
    builder = new ArgBuilder()
      .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
      .addUserMain(EDGARDocumentSuccessRate.class.getCanonicalName())
      .addUserArgs(inputFilePath, "sliding", outputFilePath);

    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARDocumentSuccessRateSliding")
      .addOptimizationPolicy(StreamingPolicy.class.getCanonicalName())
      .build());

    // try {
    //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    // } finally {
    ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    // }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testEDGARRequestsByCIKFixed() throws Exception {
    builder = new ArgBuilder()
      .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
      .addUserMain(EDGARRequestsByCIK.class.getCanonicalName())
      .addUserArgs(inputFilePath, "fixed", outputFilePath);

    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARRequestsByCIKFixed")
      .addOptimizationPolicy(StreamingPolicy.class.getCanonicalName())
      .build());

    // try {
    //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    // } finally {
    ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    // }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testEDGARRequestsByCIKSliding() throws Exception {
    builder = new ArgBuilder()
      .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
      .addUserMain(EDGARRequestsByCIK.class.getCanonicalName())
      .addUserArgs(inputFilePath, "sliding", outputFilePath);

    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARRequestsByCIKSliding")
      .addOptimizationPolicy(StreamingPolicy.class.getCanonicalName())
      .build());

    // try {
    //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    // } finally {
    ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    // }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testEDGARTop10BadRefererDocsFixed() throws Exception {

    builder = new ArgBuilder()
      .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
      .addUserMain(EDGARTop10BadRefererDocs.class.getCanonicalName())
      .addUserArgs(inputFilePath, "fixed", outputFilePath);

    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARTop10BadRefererDocsFixed")
      .addOptimizationPolicy(StreamingPolicy.class.getCanonicalName())
      .build());

    // try {
    //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    // } finally {
    ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    // }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testEDGARTop10BadRefererDocsSliding() throws Exception {
    builder = new ArgBuilder()
      .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
      .addUserMain(EDGARTop10BadRefererDocs.class.getCanonicalName())
      .addUserArgs(inputFilePath, "sliding", outputFilePath);

    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARTop10BadRefererDocsSliding")
      .addOptimizationPolicy(StreamingPolicy.class.getCanonicalName())
      .build());

    // try {
    //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    // } finally {
    ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    // }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testEDGARTop10DocumentsFixed() throws Exception {
    builder = new ArgBuilder()
      .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
      .addUserMain(EDGARTop10Documents.class.getCanonicalName())
      .addUserArgs(inputFilePath, "fixed", outputFilePath);

    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARTop10DocumentsFixed")
      .addOptimizationPolicy(StreamingPolicy.class.getCanonicalName())
      .build());

    // try {
    //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    // } finally {
    ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    // }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testEDGARTop10DocumentsSliding() throws Exception {
    builder = new ArgBuilder()
      .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
      .addUserMain(EDGARTop10Documents.class.getCanonicalName())
      .addUserArgs(inputFilePath, "sliding", outputFilePath);

    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARTop10DocumentsSliding")
      .addOptimizationPolicy(StreamingPolicy.class.getCanonicalName())
      .build());

    // try {
    //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    // } finally {
    ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    // }
  }

  // @Test(timeout = ExampleTestArgs.TIMEOUT)
  // public void testEDGARAvgDocSizeFixedHierarchical() throws Exception {
  //   builder = new ArgBuilder()
  //     .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
  //     .addUserMain(EDGARAvgDocSize.class.getCanonicalName())
  //     .addUserArgs(inputFilePath, "fixed", outputFilePath);
  //
  //   JobLauncher.main(builder
  //     .addResourceJson(executorResourceFileName)
  //     .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARAvgDocSizeFixed")
  //     .addOptimizationPolicy(IntermediateAccumulatorPolicy.class.getCanonicalName())
  //     .build());
  //
  //   // try {
  //   //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
  //   // } finally {
  //   ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   // }
  // }
  //
  // @Test(timeout = ExampleTestArgs.TIMEOUT)
  // public void testEDGARAvgDocSizeSlidingHierarchical() throws Exception {
  //   builder = new ArgBuilder()
  //     .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
  //     .addUserMain(EDGARAvgDocSize.class.getCanonicalName())
  //     .addUserArgs(inputFilePath, "sliding", outputFilePath);
  //
  //   JobLauncher.main(builder
  //     .addResourceJson(executorResourceFileName)
  //     .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARAvgDocSizeSliding")
  //     .addOptimizationPolicy(IntermediateAccumulatorPolicy.class.getCanonicalName())
  //     .build());
  //
  //   // try {
  //   //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedSlidingWindowOutputFileName);
  //   // } finally {
  //   ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   // }
  // }
  //
  // @Test(timeout = ExampleTestArgs.TIMEOUT)
  // public void testEDGARDocumentSuccessRateFixedHierarchical() throws Exception {
  //   builder = new ArgBuilder()
  //     .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
  //     .addUserMain(EDGARDocumentSuccessRate.class.getCanonicalName())
  //     .addUserArgs(inputFilePath, "fixed", outputFilePath);
  //
  //   JobLauncher.main(builder
  //     .addResourceJson(executorResourceFileName)
  //     .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARDocumentSuccessRateFixed")
  //     .addOptimizationPolicy(IntermediateAccumulatorPolicy.class.getCanonicalName())
  //     .build());
  //
  //   // try {
  //   //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
  //   // } finally {
  //   ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   // }
  // }
  //
  // @Test(timeout = ExampleTestArgs.TIMEOUT)
  // public void testEDGARDocumentSuccessRateSlidingHierarchical() throws Exception {
  //   builder = new ArgBuilder()
  //     .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
  //     .addUserMain(EDGARDocumentSuccessRate.class.getCanonicalName())
  //     .addUserArgs(inputFilePath, "sliding", outputFilePath);
  //
  //   JobLauncher.main(builder
  //     .addResourceJson(executorResourceFileName)
  //     .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARDocumentSuccessRateSliding")
  //     .addOptimizationPolicy(IntermediateAccumulatorPolicy.class.getCanonicalName())
  //     .build());
  //
  //   // try {
  //   //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
  //   // } finally {
  //   ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   // }
  // }
  //
  // @Test(timeout = ExampleTestArgs.TIMEOUT)
  // public void testEDGARRequestsByCIKFixedHierarchical() throws Exception {
  //   builder = new ArgBuilder()
  //     .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
  //     .addUserMain(EDGARRequestsByCIK.class.getCanonicalName())
  //     .addUserArgs(inputFilePath, "fixed", outputFilePath);
  //
  //   JobLauncher.main(builder
  //     .addResourceJson(executorResourceFileName)
  //     .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARRequestsByCIKFixed")
  //     .addOptimizationPolicy(IntermediateAccumulatorPolicy.class.getCanonicalName())
  //     .build());
  //
  //   // try {
  //   //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
  //   // } finally {
  //   ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   // }
  // }
  //
  // @Test(timeout = ExampleTestArgs.TIMEOUT)
  // public void testEDGARRequestsByCIKSlidingHierarchical() throws Exception {
  //   builder = new ArgBuilder()
  //     .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
  //     .addUserMain(EDGARRequestsByCIK.class.getCanonicalName())
  //     .addUserArgs(inputFilePath, "sliding", outputFilePath);
  //
  //   JobLauncher.main(builder
  //     .addResourceJson(executorResourceFileName)
  //     .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARRequestsByCIKSliding")
  //     .addOptimizationPolicy(IntermediateAccumulatorPolicy.class.getCanonicalName())
  //     .build());
  //
  //   // try {
  //   //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
  //   // } finally {
  //   ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   // }
  // }
  //
  // @Test(timeout = ExampleTestArgs.TIMEOUT)
  // public void testEDGARTop10BadRefererDocsFixedHierarchical() throws Exception {
  //
  //   builder = new ArgBuilder()
  //     .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
  //     .addUserMain(EDGARTop10BadRefererDocs.class.getCanonicalName())
  //     .addUserArgs(inputFilePath, "fixed", outputFilePath);
  //
  //   JobLauncher.main(builder
  //     .addResourceJson(executorResourceFileName)
  //     .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARTop10BadRefererDocsFixed")
  //     .addOptimizationPolicy(IntermediateAccumulatorPolicy.class.getCanonicalName())
  //     .build());
  //
  //   // try {
  //   //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
  //   // } finally {
  //   ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   // }
  // }
  //
  // @Test(timeout = ExampleTestArgs.TIMEOUT)
  // public void testEDGARTop10BadRefererDocsSlidingHierarchical() throws Exception {
  //   builder = new ArgBuilder()
  //     .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
  //     .addUserMain(EDGARTop10BadRefererDocs.class.getCanonicalName())
  //     .addUserArgs(inputFilePath, "sliding", outputFilePath);
  //
  //   JobLauncher.main(builder
  //     .addResourceJson(executorResourceFileName)
  //     .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARTop10BadRefererDocsSliding")
  //     .addOptimizationPolicy(IntermediateAccumulatorPolicy.class.getCanonicalName())
  //     .build());
  //
  //   // try {
  //   //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
  //   // } finally {
  //   ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   // }
  // }
  //
  // @Test(timeout = ExampleTestArgs.TIMEOUT)
  // public void testEDGARTop10DocumentsFixedHierarchical() throws Exception {
  //   builder = new ArgBuilder()
  //     .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
  //     .addUserMain(EDGARTop10Documents.class.getCanonicalName())
  //     .addUserArgs(inputFilePath, "fixed", outputFilePath);
  //
  //   JobLauncher.main(builder
  //     .addResourceJson(executorResourceFileName)
  //     .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARTop10DocumentsFixed")
  //     .addOptimizationPolicy(IntermediateAccumulatorPolicy.class.getCanonicalName())
  //     .build());
  //
  //   // try {
  //   //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
  //   // } finally {
  //   ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   // }
  // }
  //
  //
  // @Test(timeout = ExampleTestArgs.TIMEOUT)
  // public void testEDGARTop10DocumentsSlidingHierarchical() throws Exception {
  //   builder = new ArgBuilder()
  //     .addScheduler("org.apache.nemo.runtime.master.scheduler.StreamingScheduler")
  //     .addUserMain(EDGARTop10Documents.class.getCanonicalName())
  //     .addUserArgs(inputFilePath, "sliding", outputFilePath);
  //
  //   JobLauncher.main(builder
  //     .addResourceJson(executorResourceFileName)
  //     .addJobId(EDGARITCase.class.getSimpleName() + "testEDGARTop10DocumentsSliding")
  //     .addOptimizationPolicy(IntermediateAccumulatorPolicy.class.getCanonicalName())
  //     .build());
  //
  //   // try {
  //   //   ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
  //   // } finally {
  //   ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
  //   // }
  // }
}
