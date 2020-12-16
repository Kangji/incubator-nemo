package org.apache.nemo.runtime.executor.checkpoint;

import org.apache.nemo.common.punctuation.Checkpointmark;
import org.apache.nemo.runtime.executor.task.DataFetcher;

import java.util.HashMap;
import java.util.Map;

public final class CheckpointBoard {
  private Map<DataFetcher, Boolean> dataFetcherToCheckpointMark = new HashMap<>();
  private int pendingCheckpointId = -1;
  private int completedCheckpointId;
  private boolean isFinished = false;

  // accessed by network thread (from checkpoint Aligner, single-threaded)
  public synchronized boolean isFinished(final Checkpointmark checkpointMark) {
    //if (checkpointMark.getCheckpointID() != pendingCheckpointId) {
    //  throw new RuntimeException();
    //}
    return isFinished;
  }

  // initialize the board
  public void initialize(final DataFetcher dataFetcher) {
    dataFetcherToCheckpointMark.put(dataFetcher, false);
  }

  // accessed by task executor thread (single)
  public synchronized void update(final DataFetcher dataFetcher, final Checkpointmark checkpointmark) {
    if (pendingCheckpointId < checkpointmark.getCheckpointID()) {
      pendingCheckpointId = checkpointmark.getCheckpointID();
      dataFetcherToCheckpointMark.replaceAll((key, val) -> false);
    }
    System.out.printf("Current Thread : %s updating!", Thread.currentThread());
    System.out.println();
    dataFetcherToCheckpointMark.put(dataFetcher, true);
    if (!dataFetcherToCheckpointMark.containsValue(false)) {
      isFinished = true;
      completedCheckpointId = checkpointmark.getCheckpointID();
      this.notifyAll();
    }
  }
}
