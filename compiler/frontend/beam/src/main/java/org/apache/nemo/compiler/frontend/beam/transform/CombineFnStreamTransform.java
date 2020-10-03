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

import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.NoWatermarkEmitTransform;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.runners.core.InMemoryStateInternals.InMemoryCombiningState;

import java.util.*;

/**
 *
 * @param <K> Key type.
 * @param <I> Input type.
 * @param <O> Output type.
 */
public final class CombineFnStreamTransform<K, I, O>
  extends AbstractCombineTransform<KeyedWorkItem<K, I>, KV<K, O>> {
  private static final Logger LOG = LoggerFactory.getLogger(CombineFnStreamTransform.class.getName());

  private final Map<K, List<WindowedValue<KV<K,I>>>> keyToValues;
  private OutputCollector<WindowedValue<KV<K, O>>> outputCollector;

  // private final ReduceFnRunner<K, I, O, ? extends BoundedWindow> reduceRunner;

  // fields to support beam Streaming
  private Watermark prevOutputWatermark;
  private final Map<K, Watermark> keyAndWatermarkHoldMap;
  private transient InMemoryTimerInternalsFactory inMemoryTimerInternalsFactory;
  private transient InMemoryStateInternalsFactory inMemoryStateInternalsFactory;
  private boolean dataReceived = false;
  private SystemReduceFn<K, I, ?, O,BoundedWindow> reduceFn;
  private Combine.CombineFn combineFn;

  // null arguments when calling methods of this variable, since we don't support sideinputs yet.
  // private final GlobalCombineFnRunner<I, A, ?> combineFnRunner;

  /**
   * Constructor.
   */
  public CombineFnStreamTransform(final Map<TupleTag<?>, Coder<?>> outputCoders,
                                   final TupleTag<KV<K, O>> mainOutputTag,
                                   final WindowingStrategy<?, ?> windowingStrategy,
                                   final PipelineOptions options,
                                   final SystemReduceFn reduceFn,
                                   final DisplayData displayData,
                                   final Combine.CombineFn combineFn) {
    super(null, /* doFn */
      null, /* inputCoder */
      outputCoders,
      mainOutputTag,
      Collections.emptyList(),
      windowingStrategy,
      Collections.emptyMap(),
      options,
      displayData,
      DoFnSchemaInformation.create(),
      Collections.emptyMap());
    this.reduceFn = reduceFn;
    this.prevOutputWatermark = new Watermark(Long.MIN_VALUE);
    this.keyAndWatermarkHoldMap = new HashMap<>();
    this.keyToValues = new HashMap<>();
    this.combineFn = combineFn;
  }

  @Override
  protected DoFn wrapDoFn(final DoFn doFn) {
    final Map<K, CombineFnStreamTransform.StateAndTimerForKey> map = new HashMap<>();
    this.inMemoryStateInternalsFactory = new CombineFnStreamTransform.InMemoryStateInternalsFactory(map);
    this.inMemoryTimerInternalsFactory = new CombineFnStreamTransform.InMemoryTimerInternalsFactory(map);

    // This function performs group by key and window operation
    return
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        getWindowingStrategy(),
        inMemoryStateInternalsFactory,
        inMemoryTimerInternalsFactory,
        getSideInputReader(),
        reduceFn,
        getOutputManager(),
        getMainOutputTag());
  }

  @Override
  protected DoFnRunner<KeyedWorkItem<K, I>, KV<K, O>> wrapDoFnRunner(
    DoFnRunner<KeyedWorkItem<K, I>, KV<K, O>> currRunner, StepContext stepContext) {
    return DoFnRunners.lateDataDroppingRunner((DoFnRunner) getDoFnRunner(), new InMemoryTimerInternals(), getWindowingStrategy());
  }

  @Override
  protected OutputCollector wrapOutputCollector(OutputCollector oc) {
    return outputCollector;
  }
  public void onData(final WindowedValue<KeyedWorkItem<K, I>> element) {
    checkAndInvokeBundle();

    checkAndFinishBundle();
    return;
  }

  /**
  @Override
  public void close() {
    final Iterator<Map.Entry<K, O>> iterator = keyToAccumulator.entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<K, O> entry = iterator.next();
      final K key = entry.getKey();
      final A accum = entry.getValue();
      final A compactAccum = combineFnRunner.compact(accum, null, null, null);
      outputCollector.emit(WindowedValue.valueInGlobalWindow(KV.of(key, compactAccum)));
      iterator.remove(); // for eager garbage collection
    }
  }
   */

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CombineFnStreamTransform:");
    sb.append(super.toString());
    return sb.toString();
  }


  /**
   * Process the collected data and trigger timers.
   *
   * @param inputWatermark   current input watermark
   * @param processingTime   processing time
   * @param synchronizedTime synchronized time
   */
  private void processElementsAndTriggerTimers (final Watermark inputWatermark,
                                               final org.joda.time.Instant processingTime,
                                               final org.joda.time.Instant synchronizedTime) {
    for (final Map.Entry<K, List<WindowedValue<KV<K,I>>>> entry : keyToValues.entrySet()) {
      final K key = entry.getKey();
      final List<WindowedValue<KV<K, I>>> values = entry.getValue();
      //final ReduceFnRunner<?, ?, ?, ? extends BoundedWindow> curr_runner = getreduceRunner();

      // for each key
      // Process elements
      if (values != null && values.size() > 0) {
        //curr_runner.processElements(entry.getValue());
      }
      // Trigger timers
      triggerTimers(key, inputWatermark, processingTime, synchronizedTime);
      // Remove values
      values.clear();
    }
  }

  /**
   * Output watermark
   * = max(prev output watermark,
   * min(input watermark, watermark holds)).
   *
   * @param inputWatermark input watermark
   */
  private void emitOutputWatermark(final Watermark inputWatermark) {
    // Find min watermark hold
    final Watermark minWatermarkHold = keyAndWatermarkHoldMap.isEmpty()
      ? new Watermark(dataReceived ? Long.MIN_VALUE : Long.MAX_VALUE)
      // set this to MAX, in order not to emit input watermark when there are no outputs.
      : Collections.min(keyAndWatermarkHoldMap.values());
    final Watermark outputWatermarkCandidate = new Watermark(
      Math.max(prevOutputWatermark.getTimestamp(),
        Math.min(minWatermarkHold.getTimestamp(), inputWatermark.getTimestamp())));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Watermark hold: {}, "
        + "inputWatermark: {}, outputWatermark: {}", minWatermarkHold, inputWatermark, prevOutputWatermark);
    }

    if (outputWatermarkCandidate.getTimestamp() > prevOutputWatermark.getTimestamp()) {
      // progress!
      prevOutputWatermark = outputWatermarkCandidate;
      // emit watermark
      getOutputCollector().emitWatermark(outputWatermarkCandidate);
      // Remove minimum watermark holds
      if (minWatermarkHold.getTimestamp() == outputWatermarkCandidate.getTimestamp()) {
        keyAndWatermarkHoldMap.entrySet()
          .removeIf(entry -> entry.getValue().getTimestamp() == minWatermarkHold.getTimestamp());
      }
    }
  }

  @Override
  public void onWatermark(final Watermark inputWatermark) {
    checkAndInvokeBundle();
    processElementsAndTriggerTimers(inputWatermark, org.joda.time.Instant.now(), org.joda.time.Instant.now());
    // Emit watermark to downstream operators
    emitOutputWatermark(inputWatermark);
    checkAndFinishBundle();
  }

  /**
   * This advances the input watermark and processing time to the timestamp max value
   * in order to emit all data.
   */
  @Override
  protected void beforeClose() {
    // Finish any pending windows by advancing the input watermark to infinity.
    processElementsAndTriggerTimers(new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()),
      BoundedWindow.TIMESTAMP_MAX_VALUE, BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  /**
   * Trigger times for current key.
   * When triggering, it emits the windowed data to downstream operators.
   *
   * @param key              key
   * @param watermark        watermark
   * @param processingTime   processing time
   * @param synchronizedTime synchronized time
   */
  private void triggerTimers(final K key,
                             final Watermark watermark,
                             final org.joda.time.Instant processingTime,
                             final org.joda.time.Instant synchronizedTime) {
    final InMemoryTimerInternals timerInternals = (InMemoryTimerInternals)
      inMemoryTimerInternalsFactory.timerInternalsForKey(key);
    try {
      timerInternals.advanceInputWatermark(new org.joda.time.Instant(watermark.getTimestamp()));
      timerInternals.advanceProcessingTime(processingTime);
      timerInternals.advanceSynchronizedProcessingTime(synchronizedTime);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    final List<TimerInternals.TimerData> timerDataList = getEligibleTimers(timerInternals);

    if (!timerDataList.isEmpty()) {
      // Trigger timers and emit windowed data
      final KeyedWorkItem<K, I> timerWorkItem =
        KeyedWorkItems.timersWorkItem(key, timerDataList);
      // The DoFnRunner interface requires WindowedValue,
      // but this windowed value is actually not used in the ReduceFnRunner internal.
      getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(timerWorkItem));
    }
  }

  /**
   * Get timer data.
   *
   * @param timerInternals in-memory timer internals.
   * @return list of timer datas.
   */
  private List<TimerInternals.TimerData> getEligibleTimers(final InMemoryTimerInternals timerInternals) {
    final List<TimerInternals.TimerData> timerData = new LinkedList<>();

    while (true) {
      TimerInternals.TimerData timer;
      boolean hasFired = false;

      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      if (!hasFired) {
        break;
      }
    }

    return timerData;
  }

  /**
   * State and timer internal.
   */
  final class StateAndTimerForKey {
    private StateInternals stateInternals;
    private TimerInternals timerInternals;

    /**
     * @param stateInternals state internals.
     * @param timerInternals timer internals.
     */
    StateAndTimerForKey(final InMemoryCombiningState stateInternals,
                        final TimerInternals timerInternals) {
      //this.stateInternals = stateInternals;
      this.timerInternals = timerInternals;
    }
  }

  /**
   * InMemoryStateInternalsFactory.
   */
  final class InMemoryStateInternalsFactory implements StateInternalsFactory<K> {
    private final Map<K, CombineFnStreamTransform.StateAndTimerForKey> map;

    /**
     * @param map initial map.
     */
    InMemoryStateInternalsFactory(final Map<K, CombineFnStreamTransform.StateAndTimerForKey> map) {
      this.map = map;
    }

    @Override
    public StateInternals stateInternalsForKey(final K key) {
      StateAndTimerForKey curr = map.get(key);
      if (curr == null) {
        //map.add(key, CombineFnStreamTransform.StateAndTimerForKey(new InMemoryCombiningState<>(combineFn, combineFn.getAccumulatorCoder()), null));
      }
      else if (curr.stateInternals == null) {
        //curr.stateInternals = new InMemoryCombiningState<>(combineFn,combineFn.getAccumulatorCoder());
      }
      return curr.stateInternals;
    }
  }

  /**
   * InMemoryTimerInternalsFactory.
   */
  final class InMemoryTimerInternalsFactory implements TimerInternalsFactory<K> {
    private final Map<K, CombineFnStreamTransform.StateAndTimerForKey> map;

    /**
     * @param map initial map.
     */
    InMemoryTimerInternalsFactory(final Map<K, CombineFnStreamTransform.StateAndTimerForKey> map) {
      this.map = map;
    }

    @Override
    public TimerInternals timerInternalsForKey(final K key) {
      map.putIfAbsent(key, new CombineFnStreamTransform.StateAndTimerForKey(null, new InMemoryTimerInternals()));
      final CombineFnStreamTransform.StateAndTimerForKey stateAndTimerForKey = map.get(key);
      if (stateAndTimerForKey.timerInternals == null) {
        stateAndTimerForKey.timerInternals = new InMemoryTimerInternals();
      }
      return stateAndTimerForKey.timerInternals;
    }
  }

  /**
   * This class wraps the output collector to track the watermark hold of each key.
   */
  final class CBOutputCollector extends AbstractOutputCollector<WindowedValue<KV<K, O>>> {
    private final OutputCollector<WindowedValue<KV<K, O>>> outputCollector;

    /**
     * @param outputCollector output collector.
     */
    CBOutputCollector(final OutputCollector<WindowedValue<KV<K, O>>> outputCollector) {
      this.outputCollector = outputCollector;
    }

    @Override
    public void emit(final WindowedValue<KV<K, O>> output) {

      // The watermark advances only in ON_TIME
      if (output.getPane().getTiming().equals(PaneInfo.Timing.ON_TIME)) {
        final K key = output.getValue().getKey();
        final InMemoryTimerInternals timerInternals = (InMemoryTimerInternals)
          inMemoryTimerInternalsFactory.timerInternalsForKey(key);
        keyAndWatermarkHoldMap.put(key,
          // adds the output timestamp to the watermark hold of each key
          // +1 to the output timestamp because if the window is [0-5000), the timestamp is 4999
          new Watermark(output.getTimestamp().getMillis() + 1));
        timerInternals.advanceOutputWatermark(new Instant(output.getTimestamp().getMillis() + 1));
      }
      outputCollector.emit(output);
    }

    @Override
    public void emitWatermark(final Watermark watermark) {
      outputCollector.emitWatermark(watermark);
    }

    @Override
    public <T> void emit(final String dstVertexId, final T output) {
      outputCollector.emit(dstVertexId, output);
    }
  }
}
