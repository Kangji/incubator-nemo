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
package edu.snu.nemo.runtime.executor.data.block;

import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.common.exception.BlockWriteException;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.executor.data.DataUtil;
import edu.snu.nemo.runtime.executor.data.partition.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.partition.Partition;
import edu.snu.nemo.runtime.executor.data.partition.SerializedPartition;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * This class represents a block which is stored in local memory and not serialized.
 * Concurrent read is supported, but concurrent write is not supported.
 *
 * @param <K> the key type of its partitions.
 */
@NotThreadSafe
public final class NonSerializedMemoryBlock<K extends Serializable> extends AbstractBlock<K> {

  private final List<NonSerializedPartition<K>> nonSerializedPartitions;
  private volatile boolean committed;

  /**
   * Constructor.
   * If write (or read) as bytes is enabled, data written to (read from) this block does not (de)serialized.
   *
   * @param blockId      the ID of this block.
   * @param serializer   the {@link Serializer}.
   */
  public NonSerializedMemoryBlock(final String blockId,
                                  final Serializer serializer) {
    super(blockId, serializer);
    this.nonSerializedPartitions = new ArrayList<>();
    this.committed = false;
  }

  /**
   * Writes an element to non-committed block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param key     the key.
   * @param element the element to write.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public void write(final K key,
                    final Object element) throws BlockWriteException {
    if (committed) {
      throw new BlockWriteException(new Throwable("The block is already committed!"));
    } else {
      try {
        final NonSerializedPartition<K> partition = (NonSerializedPartition<K>)
            getNonCommittedPartitionsMap().computeIfAbsent(key, absentKey -> new NonSerializedPartition<>(key));
        partition.write(element);
      } catch (final IOException e) {
        throw new BlockWriteException(e);
      }
    }
  }

  /**
   * Stores {@link NonSerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param partitions the {@link NonSerializedPartition}s to store.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public void writePartitions(final Iterable<NonSerializedPartition<K>> partitions) throws BlockWriteException {
    if (!committed) {
      partitions.forEach(nonSerializedPartitions::add);
    } else {
      throw new BlockWriteException(new Throwable("Cannot append partition to the committed block"));
    }
  }

  /**
   * Stores {@link SerializedPartition}s to this block.
   * Because all data in this block is stored in a non-serialized form,
   * the data in these partitions have to be deserialized.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public void writeSerializedPartitions(final Iterable<SerializedPartition<K>> partitions) throws BlockWriteException {
    if (!committed) {
      try {
        final Iterable<NonSerializedPartition<K>> convertedPartitions =
            DataUtil.convertToNonSerPartitions(getSerializer(), partitions);
        writePartitions(convertedPartitions);
      } catch (final IOException e) {
        throw new BlockWriteException(e);
      }
    } else {
      throw new BlockWriteException(new Throwable("Cannot append partitions to the committed block"));
    }
  }

  /**
   * Retrieves the {@link NonSerializedPartition}s in a specific hash range from this block.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the hash range to retrieve.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   */
  @Override
  public Iterable<NonSerializedPartition<K>> readPartitions(final KeyRange keyRange) throws BlockFetchException {
    if (committed) {
      // Retrieves data in the hash range from the target block
      final List<NonSerializedPartition<K>> retrievedPartitions = new ArrayList<>();
      nonSerializedPartitions.forEach(partition -> {
        final K key = partition.getKey();
        if (keyRange.includes(key)) {
          retrievedPartitions.add(partition);
        }
      });

      return retrievedPartitions;
    } else {
      throw new BlockFetchException(new Throwable("Cannot retrieve elements before a block is committed"));
    }
  }

  /**
   * Retrieves the {@link SerializedPartition}s in a specific hash range.
   * Because the data is stored in a non-serialized form, it have to be serialized.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   */
  @Override
  public Iterable<SerializedPartition<K>> readSerializedPartitions(final KeyRange keyRange) throws BlockFetchException {
    try {
      return DataUtil.convertToSerPartitions(getSerializer(), readPartitions(keyRange));
    } catch (final IOException e) {
      throw new BlockFetchException(e);
    }
  }

  /**
   * Commits this block to prevent further write.
   *
   * @return empty optional because the data is not serialized.
   */
  @Override
  public synchronized Optional<Map<K, Long>> commit() {
    if (!committed) {
      commitPartitions();
      committed = true;
    }
    return Optional.empty();
  }

  @Override
  public void commitPartitions() throws BlockWriteException {
    try {
      for (final Partition<?, K> partition : getNonCommittedPartitionsMap().values()) {
        partition.commit();
        nonSerializedPartitions.add((NonSerializedPartition<K>) partition);
      }
      getNonCommittedPartitionsMap().clear();
    } catch (final IOException e) {
      throw new BlockWriteException(e);
    }
  }

  /**
   * @return whether this block is committed or not.
   */
  @Override
  public synchronized boolean isCommitted() {
    return committed;
  }
}
