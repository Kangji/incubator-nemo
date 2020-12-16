package org.apache.nemo.runtime.executor.checkpoint;

import java.util.Queue;

import org.apache.nemo.common.punctuation.Checkpointmark;

/**
 * This class aligns checkpoint marks from a data fetchers. When an input channel receives
 * a checkpoint mark, the checkpointmark aligner blocks the channel until all other input channels receive the
 * corresponding checkpoint mark.
 */

public final class CheckpointAligner {
  private int pendingCheckpointId;
  private int numOfReceivedMark = 0;
  private int numOfExpectedMark = 5;
  private boolean first = true;
  private boolean last = true;
  private final Queue elementQueue;

  public CheckpointAligner(final Queue elementQueue) {
    this.elementQueue = elementQueue;
  }

  public synchronized void processCheckpointMark(final Checkpointmark checkpointmark) {
    if (first) {
      first = false;
      last = true;
      numOfReceivedMark = 0;
      pendingCheckpointId = checkpointmark.getCheckpointID();
    }
    numOfReceivedMark++;
    try {
      while (numOfReceivedMark < numOfExpectedMark) {
        this.wait();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    if (last) {
      last = false;
      first = true;
      elementQueue.offer(checkpointmark);
      this.notifyAll();
    }
  }

  public void setNumOfExpectedMark(final int expected) {
    numOfExpectedMark = expected;
  }
}
