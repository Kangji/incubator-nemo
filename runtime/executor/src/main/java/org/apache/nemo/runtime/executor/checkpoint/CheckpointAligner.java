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
  private final CheckpointBoard checkpointBoard;

  public CheckpointAligner(final Queue elementQueue, final CheckpointBoard checkpointBoard) {
    this.elementQueue = elementQueue;
    this.checkpointBoard = checkpointBoard;
  }

  public synchronized void processCheckpointMark(final Checkpointmark checkpointmark) {
    if (first) {
      System.out.printf("Current Thread : %s First checkpointMark received!", Thread.currentThread());
      System.out.println();
      first = false;
      last = true;
      numOfReceivedMark = 0;
      pendingCheckpointId = checkpointmark.getCheckpointID();
    }
    numOfReceivedMark++;
    try {
      while (numOfReceivedMark < numOfExpectedMark) {
        System.out.printf("Current Thread : %s Waiting for other channels to receive checkpointmark!", Thread.currentThread());
        System.out.println();
        this.wait();
      }
      System.out.printf("Current Thread : %s All channels received!", Thread.currentThread());
      System.out.println();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    if (last) {
      System.out.printf("Current Thread : %s providing checkpoint mark to queue", Thread.currentThread());
      System.out.println();
      last = false;
      first = true;
      elementQueue.offer(checkpointmark);

      try {
        synchronized (checkpointBoard) {
          while (!checkpointBoard.isFinished(checkpointmark)) {
            System.out.printf("Current Thread : %s waiting for other datafetcher", Thread.currentThread());
            System.out.println();
            checkpointBoard.wait();
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      System.out.printf("Current Thread : %s Everything is aligned! Notifying other threads..", Thread.currentThread());
      System.out.println();
      this.notifyAll();
    }
  }

  public void setNumOfExpectedMark(final int expected) {
    numOfExpectedMark = expected;
  }
}
