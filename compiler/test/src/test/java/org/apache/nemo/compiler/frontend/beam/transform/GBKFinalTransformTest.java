package org.apache.nemo.compiler.frontend.beam.transform;

import junit.framework.TestCase;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing.*;
import static org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing.LATE;
import static org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class GBKFinalTransformTest extends TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyAndWindowDoFnTransformTest.class.getName());
  private final static Coder key_coder = StringUtf8Coder.of();
  private final static Coder input_coder = BigEndianIntegerCoder.of();
  private final static KvCoder<String, Integer> kv_coder = KvCoder.of(key_coder, input_coder);
  private final static Map<TupleTag<?>, Coder<?>> null_coder = null;

  private void checkOutput(final KV<String, Integer> expected, final KV<String, Integer> result) {

    // check key
    assertEquals(expected.getKey(), result.getKey());

    // check value

    assertEquals(expected.getValue(), result.getValue());
  }

  // defining Combine function
  public static class CountFn extends Combine.CombineFn<Integer, CountFn.Accum, Integer> {

    public static class Accum {
      int sum = 0;
    }

    @Override
    public Accum createAccumulator() {
      return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, Integer input) {
      accum.sum += input;
      return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
      Accum merged = createAccumulator();
      for (Accum accum : accums) {
        merged.sum += accum.sum;
      }
      return merged;
    }

    @Override
    public Integer extractOutput(Accum accum) {
      return accum.sum;
    }

    @Override
    public Coder<CountFn.Accum> getAccumulatorCoder(CoderRegistry registry, Coder<Integer> inputcoder) {
      return AvroCoder.of(CountFn.Accum.class);
    }
  }

  // Accumulator coder
  public final static Combine.CombineFn combine_fn = new CountFn();


  // window size: 2 sec
  // interval size: 1 sec
  //
  //                           [--------------window2------------------------------]
  // [----------------------- window1 --------------------------]
  // [-------window0-------]
  // ts1 -- ts2 -- ts3 -- w -- ts4 -- w2 -- ts5 --ts6 --ts7 -- w3 -- ts8 --ts9 - --w4
  // (1, "hello")
  //      (1, "world")
  //             (2, "hello")
  //                   ==> window1: {(1,["hello","world"]), (2, ["hello"])}
  //                                 (1, "a")
  //                                                       (2,"a")
  //                                                             (3,"a")
  //                                                                  (2,"b")
  //                                                       => window2: {(1,"a"), (2,["a","b"]), (3,"a")}
  @Test
  @SuppressWarnings("unchecked")
  public void test() {

    final TupleTag<String> outputTag = new TupleTag<>("main-output");
    final SlidingWindows slidingWindows = SlidingWindows.of(Duration.standardSeconds(10))
      .every(Duration.standardSeconds(5));

    AppliedCombineFn<String, Integer, CountFn.Accum, Integer> applied_combine_fn =
      AppliedCombineFn.withInputCoder(
        combine_fn,
        CoderRegistry.createDefault(),
        kv_coder,
        null,
        WindowingStrategy.of(slidingWindows)
        );

    final GBKFinalTransform<String, Integer, Integer> combine_transform =
      new GBKFinalTransform(
        input_coder,
        key_coder,
        null_coder,
        outputTag,
        WindowingStrategy.of(slidingWindows),
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.combining(key_coder, applied_combine_fn),
        DisplayData.none());

    // Timestamp
    final Instant ts1 = new Instant(1000);
    final Instant ts2 = new Instant(2000);
    final Instant ts3 = new Instant(6000);
    final Instant ts4 = new Instant(8000);
    final Instant ts5 = new Instant(11000);
    final Instant ts6 = new Instant(14000);
    final Instant ts7 = new Instant(16000);
    final Instant ts8 = new Instant(17000);
    final Instant ts9 = new Instant(19000);
    final Watermark watermark1 = new Watermark(7000);
    final Watermark watermark2 = new Watermark(12000);
    final Watermark watermark3 = new Watermark(18000);
    final Watermark watermark4 = new Watermark(21000);

    List<IntervalWindow> sortedWindows = new ArrayList<>(slidingWindows.assignWindows(ts4));
    Collections.sort(sortedWindows, IntervalWindow::compareTo);

    final IntervalWindow window1 = sortedWindows.get(0);
    final IntervalWindow window2 = sortedWindows.get(1);

    sortedWindows = new ArrayList<>(slidingWindows.assignWindows(ts9));
    Collections.sort(sortedWindows, IntervalWindow::compareTo);
    final IntervalWindow window3 = sortedWindows.get(0);


    final Transform.Context context = mock(Transform.Context.class);
    final TestOutputCollector<KV<String, Integer>> oc = new TestOutputCollector();
    combine_transform.prepare(context, oc);
    combine_transform.onData(WindowedValue.of(
      KV.of("a", 1), ts1, slidingWindows.assignWindows(ts1), PaneInfo.NO_FIRING));
    combine_transform.onData(WindowedValue.of(
      KV.of("c", 1), ts2, slidingWindows.assignWindows(ts2), PaneInfo.NO_FIRING));

    combine_transform.onData(WindowedValue.of(
      KV.of("b", 1), ts3, slidingWindows.assignWindows(ts3), PaneInfo.NO_FIRING));
    combine_transform.onWatermark(watermark1);

    oc.outputs.clear();
    oc.watermarks.clear();

    combine_transform.onData(WindowedValue.of(
      KV.of("a", 2), ts4, slidingWindows.assignWindows(ts4), PaneInfo.NO_FIRING));
    combine_transform.onData(WindowedValue.of(
      KV.of("c", 2), ts5, slidingWindows.assignWindows(ts5), PaneInfo.NO_FIRING));
    combine_transform.onWatermark(watermark2);

    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    assertEquals(Arrays.asList(window1), oc.outputs.get(0).getWindows());
    checkOutput(KV.of("a", 3), oc.outputs.get(0).getValue());
    checkOutput(KV.of("b", 1), oc.outputs.get(1).getValue());
    checkOutput(KV.of("c", 1), oc.outputs.get(2).getValue());

    oc.outputs.clear();
    oc.watermarks.clear();

    combine_transform.onData(WindowedValue.of(
      KV.of("b", 2), ts6, slidingWindows.assignWindows(ts6), PaneInfo.NO_FIRING));
    combine_transform.onData(WindowedValue.of(
      KV.of("b", 3), ts7, slidingWindows.assignWindows(ts7), PaneInfo.NO_FIRING));
    combine_transform.onWatermark(watermark2);
    combine_transform.onData(WindowedValue.of(
      KV.of("a", 3), ts8, slidingWindows.assignWindows(ts8), PaneInfo.NO_FIRING));
    combine_transform.onWatermark(watermark3);

    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    assertEquals(Arrays.asList(window2), oc.outputs.get(0).getWindows());
    checkOutput(KV.of("a", 2), oc.outputs.get(0).getValue());
    checkOutput(KV.of("b", 3), oc.outputs.get(1).getValue());
    checkOutput(KV.of("c", 2), oc.outputs.get(2).getValue());

    oc.outputs.clear();
    oc.watermarks.clear();

    combine_transform.onData(WindowedValue.of(
      KV.of("c", 3), ts9, slidingWindows.assignWindows(ts9), PaneInfo.NO_FIRING));
    combine_transform.onWatermark(watermark4);

    assertEquals(Arrays.asList(window3), oc.outputs.get(0).getWindows());
    checkOutput(KV.of("a", 3), oc.outputs.get(0).getValue());
    checkOutput(KV.of("b", 5), oc.outputs.get(1).getValue());
    checkOutput(KV.of("c", 5), oc.outputs.get(2).getValue());

    oc.outputs.clear();
    oc.watermarks.clear();
  }
}
