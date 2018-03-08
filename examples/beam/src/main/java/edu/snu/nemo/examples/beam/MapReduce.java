/*
 * Copyright (C) 2017 Seoul National University
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
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.HashMap;
import java.util.Map;

/**
 * Sample MapReduce application.
 */
public final class MapReduce {
  /**
   * Combiner implementation.
   */
  private static class Apple extends DoFn<String, KV<String, Long>> {

    private final Map<String, Long> aggregates = new HashMap<>();

    @ProcessElement
    public void processElement(final ProcessContext c) {
      // Maybe we should change this to use finishBundle?
      final String[] words = c.element().split(" +");
      final String documentId = words[0];
      final Long count = Long.parseLong(words[2]);

      final Long value = aggregates.get(documentId);
      if (value == null) {
        aggregates.put(documentId, count);
      } else {
        aggregates.put(documentId, value + count);
      }
    }

    @FinishBundle
    public void finishBundle(final FinishBundleContext c) throws Exception {
      aggregates.entrySet().forEach(entry -> {
        final String key = entry.getKey();
        final Long value = entry.getValue();
        c.output(KV.of(key, value), null, null);
      });
    }
  }

  /**
   * Private Constructor.
   */
  private MapReduce() {
  }

  /**
   * Main function for the MR BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("MapReduce");

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
/*        .apply(MapElements.<String, KV<String, Long>>via(new SimpleFunction<String, KV<String, Long>>() {
          @Override
          public KV<String, Long> apply(final String line) {
            final String[] words = line.split(" +");
            final String documentId = words[0] + "#" + words[1];
            final Long count = Long.parseLong(words[2]);
            return KV.of(documentId, count);
          }
        }))
        .apply(Combine.<String, Long, Long>perKey(Sum.ofLongs()))
*/      .apply(ParDo.of(new Apple()))
        .apply(Sum.<String>longsPerKey())
        .apply(MapElements.<KV<String, Long>, String>via(new SimpleFunction<KV<String, Long>, String>() {
          @Override
          public String apply(final KV<String, Long> kv) {
            return kv.getKey() + ": " + kv.getValue();
          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();
  }
}
