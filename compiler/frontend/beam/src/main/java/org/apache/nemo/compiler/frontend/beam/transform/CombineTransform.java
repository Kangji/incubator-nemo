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

package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CombineTransform<K, InputT, AccumT, OutputT> extends GBKTransform {
  private static final Logger LOG = LoggerFactory.getLogger(CombineTransform.class.getName());
  private final boolean isFinalCombining;
  private final boolean isFirstCombining;

  private final SystemReduceFn combineFn;
  private final SystemReduceFn intermediateCombineFn;
  private final SystemReduceFn finalReduceFn;

  private final Coder<KV<K, InputT>> inputCoder;
  private final Map<TupleTag<?>, Coder<KV<K, AccumT>>> accumulatorOutputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;

  public CombineTransform(final Coder<KV<K, InputT>> inputCoder,
                          final Map<TupleTag<?>, Coder<KV<K, AccumT>>> accumulatorOutputCoder,
                          final Map<TupleTag<?>, Coder<?>> outputCoders,
                          final TupleTag<KV<K, OutputT>> mainOutputTag,
                          final WindowingStrategy<?, ?> windowingStrategy,
                          final PipelineOptions options,
                          final SystemReduceFn combineFn,
                          final SystemReduceFn intermediateCombineFn,
                          final SystemReduceFn finalReduceFn,
                          final DoFnSchemaInformation doFnSchemaInformation,
                          final DisplayData displayData,
                          final boolean isFirstCombining,
                          final boolean isFinalCombining) {
    super(isFirstCombining ? inputCoder : accumulatorOutputCoder.values().stream().findFirst().get(),
      isFinalCombining ? outputCoders : accumulatorOutputCoder,
      mainOutputTag,
      windowingStrategy,
      options,
      isFirstCombining ? combineFn : (isFinalCombining ? finalReduceFn : intermediateCombineFn),
      doFnSchemaInformation,
      displayData);
    if (isFirstCombining && isFinalCombining) {
      throw new CompileTimeOptimizationException("Combine cannot be both first and final, use GBK instead.");
    }
    this.isFirstCombining = isFirstCombining;
    this.isFinalCombining = isFinalCombining;

    this.combineFn = combineFn;
    this.intermediateCombineFn = intermediateCombineFn;
    this.finalReduceFn = finalReduceFn;

    this.inputCoder = inputCoder;
    this.accumulatorOutputCoder = accumulatorOutputCoder;
    this.outputCoders = outputCoders;
  }

  public boolean isFirstCombining() {
    return isFirstCombining;
  }

  public boolean isFinalCombining() {
    return isFinalCombining;
  }

  public static CombineTransform getPartialCombineTransformOf(final CombineTransform ct) {
    return new CombineTransform(ct.inputCoder, ct.accumulatorOutputCoder, ct.outputCoders,
      ct.getMainOutputTag(), ct.getWindowingStrategy(), ct.options,
      ct.combineFn, ct.intermediateCombineFn, ct.finalReduceFn,
      ct.doFnSchemaInformation, ct.displayData,
      true, false);
  }

  public static CombineTransform getIntermediateCombineTransformOf(final CombineTransform ct) {
    return new CombineTransform(ct.inputCoder, ct.accumulatorOutputCoder, ct.outputCoders,
      ct.getMainOutputTag(), ct.getWindowingStrategy(), ct.options,
      ct.combineFn, ct.intermediateCombineFn, ct.finalReduceFn,
      ct.doFnSchemaInformation, ct.displayData,
      false, false);
  }

  public static CombineTransform getFinalCombineTransformOf(final CombineTransform ct) {
    return new CombineTransform(ct.inputCoder, ct.accumulatorOutputCoder, ct.outputCoders,
      ct.getMainOutputTag(), ct.getWindowingStrategy(), ct.options,
      ct.combineFn, ct.intermediateCombineFn, ct.finalReduceFn,
      ct.doFnSchemaInformation, ct.displayData,
      false, true);
  }
}
