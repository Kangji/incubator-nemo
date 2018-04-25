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
package edu.snu.nemo.runtime.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Orchestrate message sender and receiver using {@link CompletableFuture} for asynchronous request-reply communication.
 * @param <T> the type of successful reply
 */
public final class ReplyFutureMap<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ReplyFutureMap.class);
  private final ConcurrentHashMap<Long, CompletableFuture<T>> requestIdToFuture;

  public ReplyFutureMap() {
    requestIdToFuture = new ConcurrentHashMap<>();
  }

  /**
   * Called by message sender, just before a new request is sent.
   * Note that this method should be used *before* actual message sending.
   * Otherwise {@code onSuccessMessage} can be called before putting new future to {@code requestIdToFuture}.
   * @param id the request id
   * @return a {@link CompletableFuture} for the reply
   */
  public CompletableFuture<T> beforeRequest(final long id) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    LOG.info(String.format("REPLY_EXPECTED %s", id));
    requestIdToFuture.put(id, future);
    return future;
  }

  /**
   * Called by message receiver, for a successful reply message.
   * @param id the request id
   * @param successMessage the reply message
   */
  public void onSuccessMessage(final long id, final T successMessage) {
    LOG.info(String.format("REPLY_RECEIVED %s", id));
    requestIdToFuture.remove(id).complete(successMessage);
  }

  /**
   * Called for a failure in request-reply communication.
   * @param id the request id
   * @param ex throwable exception
   */
  public void onFailure(final long id, final Throwable ex) {
    requestIdToFuture.remove(id).completeExceptionally(ex);
  }
}
