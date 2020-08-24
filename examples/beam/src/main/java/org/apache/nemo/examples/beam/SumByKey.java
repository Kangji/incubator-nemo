package org.apache.nemo.examples.beam;

import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class SumByKey {
  private static final Logger LOG = LoggerFactory.getLogger(SumByKey.class.getName());

  /**
   * Private constructor.
   */
  private SumByKey() {

  }

  /**
   * Main function for the MR BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("SumByKey");

    final Pipeline p = Pipeline.create(options);

    long start = System.currentTimeMillis();

    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
      .apply(MapElements.via(new SimpleFunction<String, KV<String, String>>() {
        @Override
        public KV<String, String> apply(final String line) {
          final String[] words = line.split(" ");
          String key = words[0];
          String value = words[1];
          return KV.of(key, value);
        }
      }))
      .apply(GroupByKey.create())
      .apply(MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, String>() {
        @Override
        public String apply(final KV<String, Iterable<String>> kv) {
          final String key = kv.getKey();
          List value = Lists.newArrayList(kv.getValue());
          int sum = value.stream().mapToInt(number -> (int) number).sum();
          return key + ", " + sum;
        }
      }));
    GenericSourceSink.write(result, outputFilePath);
    p.run().waitUntilFinish();

    LOG.info("*******END*******");
    LOG.info("JCT(ms): " + (System.currentTimeMillis() - start));
  }
}
