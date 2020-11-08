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
package org.apache.nemo.runtime.executor.transfer;

import org.apache.nemo.common.punctuation.Finishmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class provides a data transfer interface to the receiver side when both the sender and the receiver are in the
 * same executor. Since the sender doesn't serialize data, the receiver doesn't need to deserialize data when retrieving
 * them.
 */
public final class LocalInputContext extends LocalTransferContext {
  private static final Logger LOG = LoggerFactory.getLogger(LocalInputContext.class.getName());
  private ConcurrentLinkedQueue queue;
  private LocalOutputContext localOutputContext;
  private boolean isClosed = false;

  /**
   * Creates a new local input context and connect it to {@param localOutputContext}.
   * @param localOutputContext the local output context to which this local input context is connected
   */
  public LocalInputContext(final LocalOutputContext localOutputContext) {
    super(localOutputContext.getExecutorId(),
          localOutputContext.getEdgeId(),
          localOutputContext.getSrcTaskIndex(),
          localOutputContext.getDstTaskIndex());
    this.localOutputContext = localOutputContext;
    this.queue = localOutputContext.getQueue();
  }

  /**
   * Closes this local input context.
   * @throws RuntimeException if the connected output context hasn't been closed yet, or if there are still data
   * left to be processed.
   */
  @Override
  public void close() throws RuntimeException {
    if (!localOutputContext.isClosed()) {
      throw new RuntimeException("The parent task writer is still sending data");
    }
    if (!queue.isEmpty()) {
      throw new RuntimeException("There are data left in this context to be processed");
    }
    // Nullify references for potential garbage collection
    queue = null;
    localOutputContext = null;
    isClosed = true;
  }

  /**
   * Checks if this context has already been closed.
   * @return true if this context has already been closed.
   */
  public boolean isClosed() {
    return isClosed;
  }

  /**
   * Creates a new iterator which iterates the receive elements from the sender.
   * @return iterator that iterates the received elements.
   */
  public LocalInputIterator getIterator() {
    return new LocalInputIterator();
  }

  /**
   * Local input iterator that iterates the received elements from the sender.
   */
  private class LocalInputIterator implements Iterator<Object> {
    @Override
    public final boolean hasNext() {
      if (isClosed) {
        return false;
      }
      while (queue.peek() == null) {
        continue;
      }
      return true;
    }

    @Override
    public final Object next() throws RuntimeException {
      if (isClosed) {
        throw new RuntimeException("This context has already been closed");
      } else {
        Object element;
        while ((element = queue.poll()) == null) {
          continue;
        }
        if (element instanceof Finishmark) {
          LocalInputContext.this.close();
        }
        return element;
      }
    }
  }
}
