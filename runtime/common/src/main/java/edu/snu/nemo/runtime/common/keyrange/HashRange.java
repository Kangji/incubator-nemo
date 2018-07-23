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

/**
 * {@link KeyRange} implementation whose key domain is co-domain of a hash function, which returns non-negative integer.
 */
public final class HashRange implements KeyRange<Integer> {

  private static final HashRange ALL = new HashRange(0, Integer.MAX_VALUE);

  private final int rangeBeginInclusive;
  private final int rangeEndExclusive;

  /**
   * Private constructor.
   *
   * @param rangeBeginInclusive point at which the hash range starts (inclusive)
   * @param rangeEndExclusive point at which the hash range ends (exclusive)
   */
  private HashRange(final int rangeBeginInclusive, final int rangeEndExclusive) {
    try {
      if (rangeBeginInclusive < 0 || rangeEndExclusive < 0) {
        throw new IllegalArgumentException("Each boundary value of the HashRange has to be non-negative integer.");
      }
      if (rangeBeginInclusive > rangeEndExclusive) {
        throw new IllegalArgumentException("rangeBeginInclusive should be no greater than rangeEndExclusive.");
      }
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("HashRange %s is invalid: %s", toString(), e.getMessage()));
    }
    this.rangeBeginInclusive = rangeBeginInclusive;
    this.rangeEndExclusive = rangeEndExclusive;
  }

  /**
   * @return a {@link HashRange} representing the whole range
   */
  public static HashRange all() {
    return ALL;
  }

  /**
   * Static initializer for {@link HashRange}.
   *
   * @param rangeStartInclusive the start of the range (inclusive)
   * @param rangeEndExclusive   the end of the range (exclusive)
   * @return A hash range representing [{@code rangeBeginInclusive}, {@code rangeEndExclusive})
   */
  public static HashRange of(final int rangeStartInclusive, final int rangeEndExclusive) {
    return new HashRange(rangeStartInclusive, rangeEndExclusive);
  }

  @Override
  public boolean isAll() {
    return equals(ALL);
  }

  @Override
  public Integer rangeBeginInclusive() {
    return rangeBeginInclusive;
  }

  @Override
  public Integer rangeEndExclusive() {
    return rangeEndExclusive;
  }

  @Override
  public boolean includes(final Integer i) {
    return i >= rangeBeginInclusive && i < rangeEndExclusive;
  }

  @Override
  public String toString() {
    return new StringBuilder("[").append(rangeBeginInclusive).append(", ")
        .append(rangeEndExclusive).append(")").toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HashRange other = (HashRange) o;
    return rangeBeginInclusive == other.rangeBeginInclusive && rangeEndExclusive == other.rangeEndExclusive;
  }

  @Override
  public int hashCode() {
    return rangeBeginInclusive + 31 * rangeEndExclusive;
  }
}
