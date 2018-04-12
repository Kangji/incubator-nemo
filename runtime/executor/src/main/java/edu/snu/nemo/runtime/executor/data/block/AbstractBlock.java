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

import edu.snu.nemo.runtime.executor.data.partition.Partition;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Serializable;
import java.util.*;

/**
 * This class represents an abstract block.
 *
 * @param <K> the key type of its partitions.
 */
@NotThreadSafe
public abstract class AbstractBlock<K extends Serializable> implements Block<K> {

  private final String id;
  private final Map<K, Partition<?, K>> nonCommittedPartitionsMap;
  private final Serializer serializer;

  /**
   * Constructor.
   *
   * @param blockId      the ID of this block.
   * @param serializer   the {@link Serializer}.
   */
  protected AbstractBlock(final String blockId,
                          final Serializer serializer) {
    this.id = blockId;
    this.nonCommittedPartitionsMap = new HashMap<>();
    this.serializer = serializer;
  }

  /**
   * @return the ID of this block.
   */
  @Override
  public final String getId() {
    return id;
  }

  protected final Map<K, Partition<?, K>> getNonCommittedPartitionsMap() {
    return nonCommittedPartitionsMap;
  }

  protected final Serializer getSerializer() {
    return serializer;
  }
}
