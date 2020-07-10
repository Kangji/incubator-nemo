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
package org.apache.nemo.compiler.frontend.beam.source;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * SourceVertex implementation for BoundedSource.
 *
 * @param <O> output type.
 */
public final class BeamBoundedSourceVertex<O> extends SourceVertex<WindowedValue<O>> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamBoundedSourceVertex.class.getName());
  private BoundedSource<O> source;
  private final DisplayData displayData;
  private final long estimatedSizeBytes;

  /**
   * Constructor of BeamBoundedSourceVertex.
   *
   * @param source      BoundedSource to read from.
   * @param displayData data to display.
   */
  public BeamBoundedSourceVertex(final BoundedSource<O> source, final DisplayData displayData) {
    this.source = source;
    this.displayData = displayData;
    try {
      this.estimatedSizeBytes = source.getEstimatedSizeBytes(null);
    } catch (Exception e) {
      throw new MetricException(e);
    }
  }

  /**
   * Constructor of BeamBoundedSourceVertex.
   *
   * @param that the source object for copying
   */
  private BeamBoundedSourceVertex(final BeamBoundedSourceVertex that) {
    super(that);
    this.source = that.source;
    this.displayData = that.displayData;
    try {
      this.estimatedSizeBytes = source.getEstimatedSizeBytes(null);
    } catch (Exception e) {
      throw new MetricException(e);
    }
  }

  @Override
  public BeamBoundedSourceVertex getClone() {
    return new BeamBoundedSourceVertex(this);
  }

  @Override
  public boolean isBounded() {
    return true;
  }

  @Override
  public List<Readable<WindowedValue<O>>> getReadables(final int desiredNumOfSplits) throws Exception {
    final List<Readable<WindowedValue<O>>> readables = new ArrayList<>();

    if (source != null) {
      LOG.info("estimate: {}", source.getEstimatedSizeBytes(null));
      LOG.info("desired: {}", desiredNumOfSplits);
      source.split(this.estimatedSizeBytes / desiredNumOfSplits, null)
        .forEach(boundedSource -> readables.add(new BoundedSourceReadable<>(boundedSource)));
      return readables;
    } else {
      // TODO #333: Remove SourceVertex#clearInternalStates
      final SourceVertex emptySourceVertex = new EmptyComponents.EmptySourceVertex("EMPTY");
      return emptySourceVertex.getReadables(desiredNumOfSplits);
    }
  }

  @Override
  // need to add execution order here?
  public List<Readable<WindowedValue<O>>> getCoalescedReadables(final int desiredNumOfSplits,
                                                                final int stageParallelism,
                                                                final boolean isInSamplingStage) throws Exception {
    final List<Readable<WindowedValue<O>>> readables = new ArrayList<>();

    if (source != null) {
      LOG.info("estimate: {}", source.getEstimatedSizeBytes(null));
      LOG.info("desired: {}", desiredNumOfSplits);
      // need to import other source code then default
      final List<BoundedSource<O>> boundedSourceList = (List<BoundedSource<O>>) source
        .split(this.estimatedSizeBytes / desiredNumOfSplits, null);

      if (isInSamplingStage) {
        // for now, let's stick with the current ad-hoc method
        //index 0-511 are for sampling
        for (int i = 0; i < 4; i++) {
          readables.add(new CoalescedBoundedSourceReadable<>(Collections.singletonList(boundedSourceList.get(i))));
        }
        for (int groupStartingIndex = 4; groupStartingIndex < 512; groupStartingIndex *= 2) {
          int sublistLength = groupStartingIndex / 4;
          for (int startIndex = groupStartingIndex; startIndex < groupStartingIndex * 2; startIndex += sublistLength) {
            readables.add(new CoalescedBoundedSourceReadable<>(
              boundedSourceList.subList(startIndex, startIndex + sublistLength)));
          }
        }
      } else {
        final int numberOfSplitsToBindTogether = (desiredNumOfSplits - 512) / stageParallelism;
        for (int i = 0; i < stageParallelism - 1; i++) {
          readables.add(new CoalescedBoundedSourceReadable<>(boundedSourceList.subList(
            512 + i * numberOfSplitsToBindTogether, 512 + (i + 1) * numberOfSplitsToBindTogether)));
        }
        // for handling possible exception
        readables.add(new CoalescedBoundedSourceReadable<>(boundedSourceList.subList(
          512 + (stageParallelism - 1) * numberOfSplitsToBindTogether, boundedSourceList.size())));
      }
    } else {
      // TODO #333: Remove SourceVertex#clearInternalStates
      final SourceVertex emptySourceVertex = new EmptyComponents.EmptySourceVertex("EMPTY");
      return emptySourceVertex.getReadables(desiredNumOfSplits);
    }
    return readables;
  }

  @Override
  public long getEstimatedSizeBytes() {
    return this.estimatedSizeBytes;
  }

  @Override
  public void clearInternalStates() {
    source = null;
  }

  @Override
  public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = getIRVertexPropertiesAsJsonNode();
    node.put("source", displayData.toString());
    return node;
  }

  /**
   * BoundedSourceReadable class.
   *
   * @param <T> type.
   */
  private static final class BoundedSourceReadable<T> implements Readable<WindowedValue<T>> {
    private final BoundedSource<T> boundedSource;
    private boolean finished = false;
    private BoundedSource.BoundedReader<T> reader;

    /**
     * Constructor of the BoundedSourceReadable.
     *
     * @param boundedSource the BoundedSource.
     */
    BoundedSourceReadable(final BoundedSource<T> boundedSource) {
      this.boundedSource = boundedSource;
    }

    @Override
    public void prepare() {
      try {
        reader = boundedSource.createReader(null);
        finished = !reader.start();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public WindowedValue<T> readCurrent() {
      if (finished) {
        throw new IllegalStateException("Bounded reader read all elements");
      }

      final T elem = reader.getCurrent();

      try {
        finished = !reader.advance();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      return WindowedValue.valueInGlobalWindow(elem);
    }

    @Override
    public long readWatermark() {
      throw new UnsupportedOperationException("No watermark");
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public List<String> getLocations() throws Exception {
      if (boundedSource instanceof HadoopFormatIO.HadoopInputFormatBoundedSource) {
        final Field inputSplitField = boundedSource.getClass().getDeclaredField("inputSplit");
        inputSplitField.setAccessible(true);
        final InputSplit inputSplit = ((HadoopFormatIO.SerializableSplit) inputSplitField
          .get(boundedSource)).getSplit();
        return Arrays.asList(inputSplit.getLocations());
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public void close() throws IOException {
      finished = true;
      reader.close();
    }
  }

  /**
   * CoalescedBoundedSourceReadable class.
   *
   * This class manages multiple sources in one readable.
   * @param <T> type.
   */
  private static final class CoalescedBoundedSourceReadable<T> implements Readable<WindowedValue<T>> {
    private final List<BoundedSource<T>> boundedSourceList;
    private boolean allFinished = false;
    private final List<Boolean> finishedList;
    private final List<BoundedSource.BoundedReader<T>> readerList;

    /**
     * Constructor of CoalescedBoundedSourceReadable.
     *
     * @param boundedSourceList   list of bounded Source to read.
     */
    CoalescedBoundedSourceReadable(final List<BoundedSource<T>> boundedSourceList) {
      this.boundedSourceList = boundedSourceList;
      this.readerList = new ArrayList<>(boundedSourceList.size());
      this.finishedList = new ArrayList<>(boundedSourceList.size());
    }

    /**
     * Prepare reading data.
     */
    @Override
    public void prepare() {
      try {
        // if at least one of the reader has started: allFinished should be false
        // if every reader has not started: allFinished should be true
        for (int i = 0; i < boundedSourceList.size(); i++) {
          BoundedSource.BoundedReader<T> reader = boundedSourceList.get(i).createReader(null);
          readerList.set(i, reader);
          finishedList.set(i, !reader.start());
        }
        allFinished = finishedList.stream().allMatch(entry -> entry);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    /**
     * Method to read current data from the source.
     * The caller should check whether the Readable is finished or not by using isFinished() method
     * before calling this method.
     *
     * @return a data read by the readable.
     */

    //TO DO: Is there a way to clean up this code?
    @Override
    public WindowedValue<T> readCurrent() {
      if (allFinished) {
        throw new IllegalStateException("Bounded reader read all elements");
      }

      int currentReaderIndex = -1;
      for (int i = 0; i < readerList.size(); i++) {
        if (finishedList.get(i)) {
          continue;
        }
        currentReaderIndex = i;
      }

      if (currentReaderIndex < 0) {
        throw new IllegalStateException("Bounded reader read all elements");
      } else {
        BoundedSource.BoundedReader<T> reader = readerList.get(currentReaderIndex);
        T elem = reader.getCurrent();
        try {
          finishedList.set(currentReaderIndex, !reader.advance());
        } catch (IOException e) {
          e.printStackTrace();
        }
        allFinished = finishedList.stream().allMatch(entry -> entry);
        return WindowedValue.valueInGlobalWindow(elem);
      }
    }

    /**
     * Read watermark.
     *
     * @return watermark
     */
    @Override
    public long readWatermark() {
      throw new UnsupportedOperationException("No watermark");
    }

    /**
     * @return true if it reads all data.
     */
    @Override
    public boolean isFinished() {
      return allFinished;
    }

    /**
     * Returns the list of locations where this readable resides.
     * Each location has a complete copy of the readable.
     *
     * @return List of locations where this readable resides
     * @throws UnsupportedOperationException when this operation is not supported
     * @throws Exception                     any other exceptions on the way
     */
    @Override
    public List<String> getLocations() throws Exception {
      final List<String> inputSplitLocationList = new ArrayList<>();
      for (BoundedSource<T> boundedSource : boundedSourceList) {
        if (boundedSource instanceof HadoopFormatIO.HadoopInputFormatBoundedSource) {
          final Field inputSplitField = boundedSource.getClass().getDeclaredField("inputSplit");
          inputSplitField.setAccessible(true);
          final InputSplit inputSplit = ((HadoopFormatIO.SerializableSplit) inputSplitField
            .get(boundedSource)).getSplit();
          inputSplitLocationList.addAll(Arrays.asList(inputSplit.getLocations()));
        } else {
          throw new UnsupportedOperationException();
        }
      }
      return inputSplitLocationList;
    }

    /**
     * Close.
     *
     * @throws IOException if file-based reader throws any.
     */
    @Override
    public void close() throws IOException {
      allFinished = true;
      for (BoundedSource.BoundedReader<T> reader : readerList) {
        reader.close();
      }
    }
  }
}

