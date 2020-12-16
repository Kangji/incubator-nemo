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
package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Checkpointmark;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.checkpoint.CheckpointAligner;
import org.apache.nemo.runtime.executor.checkpoint.CheckpointBoard;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Task thread -> fetchDataElement() -> (((QUEUE))) <- List of iterators <- queueInsertionThreads
 * <p>
 * Unlike {@link ParentTaskDataFetcher}, where the task thread directly consumes (and blocks on) iterators one by one,
 * this class spawns threads that each forwards elements from an iterator to a global queue.
 * <p>
 * This class should be used when dealing with unbounded data streams, as we do not want to be blocked on a
 * single unbounded iterator forever.
 */
@NotThreadSafe
public class MultiThreadParentTaskDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadParentTaskDataFetcher.class);

  private final InputReader readersForParentTask;
  private final ExecutorService queueInsertionThreads;

  // Non-finals (lazy fetching)
  private boolean firstFetch = true;

  private final ConcurrentLinkedQueue elementQueue;

  private long serBytes = 0;
  private long encodedBytes = 0;

  private int numOfIterators; // == numOfIncomingEdges
  private int numOfFinishMarks = 0;

  // A watermark manager
  private InputWatermarkManager inputWatermarkManager;

  // Checkpoint aligner to support fault-tolerance in streaming
  private final CheckpointAligner checkpointAligner;
  private final CheckpointBoard checkpointBoard;

  public MultiThreadParentTaskDataFetcher(final IRVertex dataSource,
                                   final InputReader readerForParentTask,
                                   final OutputCollector outputCollector,
                                   final CheckpointBoard checkpointBoard) {
    super(dataSource, outputCollector);
    this.readersForParentTask = readerForParentTask;
    this.firstFetch = true;
    this.elementQueue = new ConcurrentLinkedQueue();
    this.queueInsertionThreads = Executors.newCachedThreadPool();
    this.checkpointAligner = new CheckpointAligner(elementQueue);
    this.checkpointBoard = checkpointBoard;
    System.out.println("initialization");
    checkpointBoard.initialize(this);
  }

  @Override
  Object fetchDataElement() throws IOException {
    if (firstFetch) {
      fetchDataLazily();
      firstFetch = false;
    }

    while (true) {
      if (!checkpointBoard.canProceed(this)) {
        // Wait until the other data fetchers in the same task receive a checkpoint mark
        throw new NoSuchElementException();
      }
      final Object element = elementQueue.poll();
      if (element == null) {
        throw new NoSuchElementException();
      } else if (element instanceof Finishmark) {
        numOfFinishMarks++;
        if (numOfFinishMarks == numOfIterators) {
          return Finishmark.getInstance();
        }
      } else if (element instanceof Checkpointmark) {
        checkpointBoard.update(this, (Checkpointmark) element);
      } else {
        // else try again.
        return element;
      }
    }
  }

  private void fetchDataLazily() {
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = readersForParentTask.read();
    numOfIterators = futures.size();
    checkpointAligner.setNumOfExpectedMark(numOfIterators);

    if (numOfIterators > 1) {
      inputWatermarkManager = new MultiInputWatermarkManager(numOfIterators, new WatermarkCollector());
    } else {
      inputWatermarkManager = new SingleInputWatermarkManager(new WatermarkCollector());
    }

    futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) ->
      // A thread for each iterator
      queueInsertionThreads.submit(() -> {
        if (exception == null) {
          // Consume this iterator to the end.
          while (iterator.hasNext()) { // blocked on the iterator.
            final Object element = iterator.next();
            if (element instanceof WatermarkWithIndex) {
              // watermark element
              // the input watermark manager is accessed by multiple threads
              // so we should synchronize it
              synchronized (inputWatermarkManager) {
                final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) element;
                inputWatermarkManager.trackAndEmitWatermarks(
                  watermarkWithIndex.getIndex(), watermarkWithIndex.getWatermark());
                }
                // Checkpointmark Alignment
            } else if (element instanceof Checkpointmark) {
              checkpointAligner.processCheckpointMark((Checkpointmark) element);
            } else {
              // data element
              elementQueue.offer(element);
            }
          }
          // This iterator is finished.
          countBytesSynchronized(iterator);
          elementQueue.offer(Finishmark.getInstance());
        } else {
          LOG.error(exception.getMessage());
          throw new RuntimeException(exception);
        }
      })));
  }

  final long getSerializedBytes() {
    return serBytes;
  }

  final long getEncodedBytes() {
    return encodedBytes;
  }

  private synchronized void countBytesSynchronized(final DataUtil.IteratorWithNumBytes iterator) {
    try {
      serBytes += iterator.getNumSerializedBytes();
    } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
      serBytes = -1;
    } catch (final IllegalStateException e) {
      LOG.error("Failed to get the number of bytes of serialized data - the data is not ready yet ", e);
    }
    try {
      encodedBytes += iterator.getNumEncodedBytes();
    } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
      encodedBytes = -1;
    } catch (final IllegalStateException e) {
      LOG.error("Failed to get the number of bytes of encoded data - the data is not ready yet ", e);
    }
  }

  @Override
  public void close() throws Exception {
    queueInsertionThreads.shutdown();
  }

  /**
   * Just adds the emitted watermark to the element queue.
   * It receives the watermark from InputWatermarkManager.
   */
  private final class WatermarkCollector implements OutputCollector {
    @Override
    public void emit(final Object output) {
      throw new IllegalStateException("Should not be called");
    }

    @Override
    public void emitWatermark(final Watermark watermark) {
      elementQueue.offer(watermark);
    }

    @Override
    public void emit(final String dstVertexId, final Object output) {
      throw new IllegalStateException("Should not be called");
    }
  }
}
