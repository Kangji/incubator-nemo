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

package org.apache.nemo.common;


import java.util.*;
import java.util.stream.Collectors;

/**
 * LFU cache implementation based on http://dhruvbird.com/lfu.pdf, with some notable differences:
 * <ul>
 * <li>
 * Frequency list is stored as an array with no next/prev pointers between nodes: looping over the array should be
 * faster and more CPU-cache friendly than using an ad-hoc linked-pointers structure.
 * </li>
 * <li>
 * The max frequency is capped at the cache size to avoid creating more and more frequency list entries, and all
 * elements residing in the max frequency entry are re-positioned in the frequency entry linked set in order to put
 * most recently accessed elements ahead of less recently ones, which will be collected sooner.
 * </li>
 * <li>
 * The eviction factor determines how many elements (more specifically, the percentage of) will be evicted.
 * </li>
 * </ul>
 * As a consequence, this cache runs in *amortized* O(1) time (considering the worst case of having the lowest
 * frequency at 0 and having to evict all elements).
 *
 * @author Sergio Bossa.
 * @param <Key> key.
 * @param <Value> value.
 */
public final class LFUCache<Key, Value> implements Map<Key, Value> {

  private final Map<Key, CacheNode<Key, Value>> cache;
  private final LinkedHashSet<CacheNode<Key, Value>>[] frequencyList;
  private int lowestFrequency;
  private int maxFrequency;
  private final int maxCacheSize;
  private final float evictionFactor;

  public LFUCache(final int maxCacheSize, final float evictionFactor) {
    if (evictionFactor <= 0 || evictionFactor >= 1) {
      throw new IllegalArgumentException("Eviction factor must be greater than 0 and lesser than or equal to 1");
    }
    this.cache = new HashMap<>(maxCacheSize);
    this.frequencyList = new LinkedHashSet[maxCacheSize];
    this.lowestFrequency = 0;
    this.maxFrequency = maxCacheSize - 1;
    this.maxCacheSize = maxCacheSize;
    this.evictionFactor = evictionFactor;
    initFrequencyList();
  }

  public Value put(final Key k, final Value v) {
    Value oldValue = null;
    CacheNode<Key, Value> currentNode = cache.get(k);
    if (currentNode == null) {
      if (cache.size() == maxCacheSize) {
        doEviction();
      }
      LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[0];
      currentNode = new CacheNode<>(k, v, 0);
      nodes.add(currentNode);
      cache.put(k, currentNode);
      lowestFrequency = 0;
    } else {
      oldValue = currentNode.v;
      currentNode.v = v;
    }
    return oldValue;
  }


  public void putAll(final Map<? extends Key, ? extends Value> map) {
    for (Map.Entry<? extends Key, ? extends Value> me : map.entrySet()) {
      put(me.getKey(), me.getValue());
    }
  }

  public Value get(final Object k) {
    CacheNode<Key, Value> currentNode = cache.get(k);
    if (currentNode != null) {
      int currentFrequency = currentNode.frequency;
      if (currentFrequency < maxFrequency) {
        int nextFrequency = currentFrequency + 1;
        LinkedHashSet<CacheNode<Key, Value>> currentNodes = frequencyList[currentFrequency];
        LinkedHashSet<CacheNode<Key, Value>> newNodes = frequencyList[nextFrequency];
        moveToNextFrequency(currentNode, nextFrequency, currentNodes, newNodes);
        cache.put((Key) k, currentNode);
        if (lowestFrequency == currentFrequency && currentNodes.isEmpty()) {
          lowestFrequency = nextFrequency;
        }
      } else {
        // Hybrid with LRU: put most recently accessed ahead of others:
        LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[currentFrequency];
        nodes.remove(currentNode);
        nodes.add(currentNode);
      }
      return currentNode.v;
    } else {
      return null;
    }
  }

  public Value remove(final Object k) {
    CacheNode<Key, Value> currentNode = cache.remove(k);
    if (currentNode != null) {
      LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[currentNode.frequency];
      nodes.remove(currentNode);
      if (lowestFrequency == currentNode.frequency) {
        findNextLowestFrequency();
      }
      return currentNode.v;
    } else {
      return null;
    }
  }

  public int frequencyOf(final Key k) {
    CacheNode<Key, Value> node = cache.get(k);
    if (node != null) {
      return node.frequency + 1;
    } else {
      return 0;
    }
  }

  public void clear() {
    for (int i = 0; i <= maxFrequency; i++) {
      frequencyList[i].clear();
    }
    cache.clear();
    lowestFrequency = 0;
  }

  public Set<Key> keySet() {
    return this.cache.keySet();
  }

  public Collection<Value> values() {
    return cache.values().stream().map(cn -> cn.v).collect(Collectors.toList());
  }

  public Set<Entry<Key, Value>> entrySet() {
    final Map<Key, Value> hashMap = new HashMap<>();
    cache.values().forEach(cn -> hashMap.put(cn.k, cn.v));
    return hashMap.entrySet();
  }

  public int size() {
    return cache.size();
  }

  public boolean isEmpty() {
    return this.cache.isEmpty();
  }

  public boolean containsKey(final Object o) {
    return this.cache.containsKey(o);
  }

  public boolean containsValue(final Object o) {
    return cache.values().stream().map(cn -> cn.v).anyMatch(value -> value.equals(o));
  }

  private void initFrequencyList() {
    for (int i = 0; i <= maxFrequency; i++) {
      frequencyList[i] = new LinkedHashSet<>();
    }
  }

  private void doEviction() {
    int currentlyDeleted = 0;
    float target = maxCacheSize * evictionFactor;
    while (currentlyDeleted < target) {
      LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[lowestFrequency];
      if (nodes.isEmpty()) {
        throw new IllegalStateException("Lowest frequency constraint violated!");
      } else {
        Iterator<CacheNode<Key, Value>> it = nodes.iterator();
        while (it.hasNext() && currentlyDeleted++ < target) {
          CacheNode<Key, Value> node = it.next();
          it.remove();
          cache.remove(node.k);
        }
        if (!it.hasNext()) {
          findNextLowestFrequency();
        }
      }
    }
  }

  private void moveToNextFrequency(final CacheNode<Key, Value> currentNode, final int nextFrequency,
                                   final LinkedHashSet<CacheNode<Key, Value>> currentNodes,
                                   final LinkedHashSet<CacheNode<Key, Value>> newNodes) {
    currentNodes.remove(currentNode);
    newNodes.add(currentNode);
    currentNode.frequency = nextFrequency;
  }

  private void findNextLowestFrequency() {
    while (lowestFrequency <= maxFrequency && frequencyList[lowestFrequency].isEmpty()) {
      lowestFrequency++;
    }
    if (lowestFrequency > maxFrequency) {
      lowestFrequency = 0;
    }
  }

  /**
   * Cache node.
   * @param <Key> key.
   * @param <Value> value.
   */
  private static class CacheNode<Key, Value> {

    private final Key k;
    private Value v;
    private int frequency;

    CacheNode(final Key k, final Value v, final int frequency) {
      this.k = k;
      this.v = v;
      this.frequency = frequency;
    }

  }
}
