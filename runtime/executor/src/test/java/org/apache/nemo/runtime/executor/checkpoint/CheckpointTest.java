package org.apache.nemo.runtime.executor.checkpoint;

import junit.framework.TestCase;
import org.apache.nemo.common.punctuation.Checkpointmark;
import org.apache.nemo.runtime.executor.task.DataFetcher;
import org.apache.nemo.runtime.executor.task.MultiThreadParentTaskDataFetcher;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Unit test for {@link CheckpointAligner} and {@link CheckpointBoard}.
 */

public class CheckpointTest extends TestCase {
  private final CheckpointBoard checkpointBoard = new CheckpointBoard();
  // setup for the first data fetcher
  private final LinkedBlockingQueue elementQueue1 = new LinkedBlockingQueue();
  private final CheckpointAligner checkpointAligner1 = new CheckpointAligner(elementQueue1, checkpointBoard);
  private final DataFetcher dataFetcher1 = new MultiThreadParentTaskDataFetcher(null, null, null, checkpointBoard);
  // setup for the second data fetcher
  private final LinkedBlockingQueue elementQueue2 = new LinkedBlockingQueue();
  private final CheckpointAligner checkpointAligner2 = new CheckpointAligner(elementQueue2, checkpointBoard);
  private final DataFetcher dataFetcher2 = new MultiThreadParentTaskDataFetcher(null, null, null, checkpointBoard);

  private final Checkpointmark checkpointmark = Checkpointmark.generateCheckpointmark();

  private class task implements Runnable {
    public void run() {
      try {
        Checkpointmark receivedCheckpointMark = (Checkpointmark) elementQueue1.take();
        checkpointBoard.update(dataFetcher1, receivedCheckpointMark);
        receivedCheckpointMark = (Checkpointmark) elementQueue2.take();
        checkpointBoard.update(dataFetcher2, receivedCheckpointMark);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class fetchFrom1 implements Runnable {
    public void run() {
      checkpointAligner1.processCheckpointMark(checkpointmark);
    }
  }

  private class fetchFrom2 implements Runnable {
    public void run() {
      checkpointAligner2.processCheckpointMark(checkpointmark);
    }
  }

  @Test
  public void testCheckpoint() {
    List<Thread> list = new ArrayList<>();
    list.add(new Thread(new task()));
    list.add(new Thread(new fetchFrom1()));
    list.add(new Thread(new fetchFrom1()));
    list.add(new Thread(new fetchFrom1()));
    list.add(new Thread(new fetchFrom1()));
    list.add(new Thread(new fetchFrom1()));
    list.add(new Thread(new fetchFrom2()));
    list.add(new Thread(new fetchFrom2()));
    list.add(new Thread(new fetchFrom2()));
    list.add(new Thread(new fetchFrom2()));
    list.add(new Thread(new fetchFrom2()));
    for (Thread thread : list) {
      thread.start();
    }
    for (Thread thread : list) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }


}
