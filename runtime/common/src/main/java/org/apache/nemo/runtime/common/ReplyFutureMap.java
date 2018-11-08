/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nemo.runtime.common;

import java.util.concurrent.*;

/**
 * Orchestrate message sender and receiver using {@link CompletableFuture} for asynchronous request-reply communication.
 * @param <T> the type of successful reply
 */
public final class ReplyFutureMap<T> {

  private final ConcurrentHashMap<Long, ReplyFuture<T>> requestIdToFuture;

  public ReplyFutureMap() {
    requestIdToFuture = new ConcurrentHashMap<>();
  }

  /**
   * Called by message sender, just before a new request is sent.
   * Note that this method should be used *before* actual message sending.
   * Otherwise {@code onSuccessMessage} can be called before putting new future to {@code requestIdToFuture}.
   * @param id the request id
   * @return a {@link ReplyFuture} for the reply
   */
  public ReplyFuture<T> beforeRequest(final long id) {
    final ReplyFuture<T> future = new ReplyFuture<>();
    requestIdToFuture.put(id, future);
    return future;
  }

  /**
   * Called by message receiver, for a successful reply message.
   * @param id the request id
   * @param successMessage the reply message
   */
  public void onSuccessMessage(final long id, final T successMessage) {
    requestIdToFuture.remove(id).resolve(successMessage);
  }

  /**
   * Called for a failure in request-reply communication.
   * @param id the request id
   * @param ex throwable exception
   */
  public void onFailure(final long id, final Throwable ex) {
    requestIdToFuture.remove(id).resolveExceptionally(ex);
  }

  /**
   * Future impl.
   * @param <T> the type
   */
  public static final class ReplyFuture<T> implements Future<T> {

    private volatile boolean isDone = false;
    private T value = null;
    private Throwable exception = null;
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return isDone;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      if (!isDone) {
        latch.await();
      }
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return value;
    }

    @Override
    public T get(final long timeout, final TimeUnit timeUnit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (!isDone) {
        latch.await(timeout, timeUnit);
      }
      if (!isDone) {
        throw new TimeoutException();
      }
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return value;
    }

    public void resolve(final T v) {
      ensureNotDone();
      isDone = true;
      value = v;
      latch.countDown();
    }

    public void resolveExceptionally(final Throwable throwable) {
      ensureNotDone();
      isDone = true;
      exception = throwable;
      latch.countDown();
    }

    private void ensureNotDone() {
      if (isDone) {
        throw new RuntimeException(String.format("This ReplyFuture was already done%s",
          exception == null ? String.format(": %s", value) : String.format("exceptionally: %s", exception)));
      }
    }
  }
}
