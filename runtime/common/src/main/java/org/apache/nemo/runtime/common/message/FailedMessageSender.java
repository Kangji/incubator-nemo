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
package org.apache.nemo.runtime.common.message;

import org.apache.nemo.runtime.common.ReplyFutureMap;
import org.apache.nemo.runtime.common.comm.ControlMessage;

import java.util.concurrent.CompletableFuture;

/**
 * A message sender that failed.
 */
public final class FailedMessageSender implements MessageSender<ControlMessage.Message> {
  @Override
  public void send(final ControlMessage.Message message) {
    // Do nothing.
  }

  @Override
  public ReplyFutureMap.ReplyFuture<ControlMessage.Message> request(final ControlMessage.Message message) {
    final ReplyFutureMap.ReplyFuture<ControlMessage.Message> failed = new ReplyFutureMap.ReplyFuture<>();
    failed.resolveExceptionally(new Throwable("Failed Message Sender"));
    return failed;
  }

  @Override
  public void close() throws Exception {
    // Do nothing.
  }
}
