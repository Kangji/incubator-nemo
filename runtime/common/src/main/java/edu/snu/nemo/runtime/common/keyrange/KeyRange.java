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
package edu.snu.nemo.runtime.common.keyrange;

import java.io.Serializable;

/**
 * Represents the key range of data partitions within a block.
 *
 * @param <K> the type of key to assign to each partition
 */
public interface KeyRange<K extends Serializable> extends Serializable {

  /**
   * @return whether this represents the whole range or not
   */
  boolean isAll();

  /**
   * @return the beginning of this range (inclusive)
   */
  K rangeBeginInclusive();

  /**
   * @return the end of this range (exclusive)
   */
  K rangeEndExclusive();

  /**
   * Tests whether this includes the specified key or not.
   *
   * @param key the key to test
   * @return {@code true} if and only if this key range includes the specified value, {@code false} otherwise
   */
  boolean includes(final K key);

  /**
   * {@inheritDoc}
   * Implementations should override this method for a readable representation for this range.
   */
  @Override
  String toString();

  /**
   * {@inheritDoc}
   * Implementations should override this method for comparison.
   */
  @Override
  boolean equals(final Object o);

  /**
   * {@inheritDoc}
   * Implementations should override this method for comparison.
   */
  @Override
  int hashCode();
}
