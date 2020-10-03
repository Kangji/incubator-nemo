package org.apache.nemo.common.ir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOutputCollector<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractOutputCollector.class.getName());

  protected long inputTimestamp;

  protected volatile boolean offloading;

  public void setInputTimestamp(final long timestamp) {
    inputTimestamp = timestamp;
  }

  public long getInputTimestamp() {
    return inputTimestamp;
  }
}
