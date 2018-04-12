/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.nemo.runtime.executor.data.metadata;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Serializable;

/**
 * This class represents a metadata for a local file {@link edu.snu.nemo.runtime.executor.data.block.Block}.
 * It resides in local only, and does not synchronize globally.
 * @param <K> the key type of its partitions.
 */
@ThreadSafe
public final class LocalFileMetadata<K extends Serializable> extends FileMetadata<K> {

  /**
   * Constructor.
   * If write (or read) as bytes is enabled, data written to (read from) the block does not (de)serialized.
   *
   * @param readAsBytes  whether read data from this file as arrays of bytes or not.
   * @param writeAsBytes whether write data to this file as arrays of bytes or not.
   */
  public LocalFileMetadata(final boolean readAsBytes,
                           final boolean writeAsBytes) {
    super(readAsBytes, writeAsBytes);
  }

  /**
   * @see FileMetadata#deleteMetadata()
   */
  @Override
  public void deleteMetadata() {
    // Do nothing because this metadata is only in the local memory.
  }

  /**
   * Notifies that all writes are finished for the block corresponding to this metadata.
   */
  @Override
  public void commitBlock() {
    setCommitted(true);
  }
}
