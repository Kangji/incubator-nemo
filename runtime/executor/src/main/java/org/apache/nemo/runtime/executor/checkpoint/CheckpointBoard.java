package org.apache.nemo.runtime.executor.checkpoint;

import org.apache.nemo.common.punctuation.Checkpointmark;
import org.apache.nemo.runtime.executor.task.DataFetcher;

import java.util.HashMap;
import java.util.Map;

/**
 * This class keeps track of checkpoint status of a single task by communicating with data fetchers of the task.
 */
public final class CheckpointBoard {
  private Map<DataFetcher, Boolean> dataFetcherToCheckpointMark = new HashMap<>();
  private int pendingCheckpointId = -1;
  private int completedCheckpointId;
  private boolean pending = false;
  private boolean canProceed = true;

  // initialize the board
  public void initialize(final DataFetcher dataFetcher) {
    dataFetcherToCheckpointMark.put(dataFetcher, false);
  }
  // accessed by dataFetcher (from checkpoint Aligner, single-threaded)
  public synchronized boolean canProceed(final DataFetcher dataFetcher) {
    if (!pending) {
      return true;
    }
    return !dataFetcherToCheckpointMark.get(dataFetcher);
  }

  // accessed by task executor thread (single)
  public synchronized void update(final DataFetcher dataFetcher, final Checkpointmark checkpointmark) {
      if (pendingCheckpointId < checkpointmark.getCheckpointID()) {
        // Starting a new checkpoint
        pendingCheckpointId = checkpointmark.getCheckpointID();
        pending = true;
      }
      dataFetcherToCheckpointMark.put(dataFetcher, true);
      if (!dataFetcherToCheckpointMark.containsValue(false)) {
        // Every data fetcher has received a checkpoint mark.
        pending = false;
        dataFetcherToCheckpointMark.replaceAll((key, val) -> false);
      }
  }
}
