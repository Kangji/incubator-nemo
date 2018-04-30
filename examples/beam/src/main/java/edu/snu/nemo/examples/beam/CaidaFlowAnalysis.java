/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.examples.beam;

import edu.snu.nemo.compiler.frontend.beam.NemoPipelineOptions;
import edu.snu.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An app that analyzes data flow from Cadia network trace.
 */
public final class CaidaFlowAnalysis {
  /**
   * Private constructor.
   */
  private CaidaFlowAnalysis() {
  }

  /**
   * Main function for the Beam program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String input0FilePath = args[0];
    final String input1FilePath = args[1];
    final String outputFilePath = args[2];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("CaidaFlowAnalysis");

    final Pattern lengthPattern = Pattern.compile("Len=(\\d+)");

    final Pipeline p = Pipeline.create(options);
    final PCollection<KV<String, KV<String, Long>>> in0 = GenericSourceSink.read(p, input0FilePath)
        .apply(ParDo.of(new DoFn<String, KV<String, KV<String, Long>>>() {
          @ProcessElement
          public void processElement(final ProcessContext c) {
            final String line = c.element();
            final String[] words = line.trim().split(" +");
            final Matcher lengthMatcher = lengthPattern.matcher(line);
            if (lengthMatcher.find()) {
              c.output(KV.of(words[4], KV.of(words[2], Long.valueOf(lengthMatcher.group(1)))));
            }
          }
        }));
    final PCollection<KV<String, KV<String, Long>>> in1 = GenericSourceSink.read(p, input1FilePath)
        .apply(ParDo.of(new DoFn<String, KV<String, KV<String, Long>>>() {
          @ProcessElement
          public void processElement(final ProcessContext c) {
            final String line = c.element();
            final String[] words = line.trim().split(" +");
            final Matcher lengthMatcher = lengthPattern.matcher(line);
            if (lengthMatcher.find()) {
              c.output(KV.of(words[2], KV.of(words[4], Long.valueOf(lengthMatcher.group(1)))));
            }
          }
        }));
    final TupleTag<KV<String, Long>> tag0 = new TupleTag<>();
    final TupleTag<KV<String, Long>> tag1 = new TupleTag<>();
    final PCollection<KV<String, CoGbkResult>> joined =
        KeyedPCollectionTuple.of(tag0, in0).and(tag1, in1).apply(CoGroupByKey.create());
    final PCollection<String> result = joined
        .apply(MapElements.via(new SimpleFunction<KV<String, CoGbkResult>, String>() {
          @Override
          public String apply(final KV<String, CoGbkResult> kv) {
            final Iterable<KV<String, Long>> source = kv.getValue().getAll(tag0);
            final Iterable<KV<String, Long>> destination = kv.getValue().getAll(tag1);
            final String intermediate = kv.getKey();
            return new StringBuilder(intermediate).append(",").append(stdev(source)).append(",")
                .append(stdev(destination)).toString();
          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();
  }

  private static double stdev(final Iterable<KV<String, Long>> data) {
    long num = 0;
    long sum = 0;
    long squareSum = 0;
    for (final KV<String, Long> e : data) {
      final long element = e.getValue();
      num++;
      sum += element;
      squareSum += (element * element);
    }
    if (num == 0) {
      return Double.NaN;
    }
    final double average = ((double) sum) / num;
    final double squareAverage = ((double) squareSum) / num;
    return Math.sqrt(squareAverage - average * average);
  }
}
