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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Sample MapReduce application.
 */
public final class MapReduce {
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
        .apply(MapElements.via(new SimpleFunction<String, KV<String, String>>() {
          @Override
          public KV<String, String> apply(final String line) {
            final String[] words = line.trim().split(" +");
            return KV.of(words[2], words[4]);
          }
        }))
        .apply(GroupByKey.create())
        .apply(MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, String>() {
          @Override
          public String apply(final KV<String, Iterable<String>> kv) {

            final StringBuilder builder = new StringBuilder(kv.getKey()).append(":");
            boolean isFirst = true;
            for (final String element : kv.getValue()) {
              if (isFirst) {
                isFirst = false;
              } else {
                builder.append(",");
              }
              builder.append(element);
            }
            return builder.toString();

          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();
  }
}
