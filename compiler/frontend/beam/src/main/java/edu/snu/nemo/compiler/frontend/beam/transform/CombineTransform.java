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
package edu.snu.nemo.compiler.frontend.beam.transform;

import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import org.apache.beam.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Combine Beam elements.
 * @param <T> input type.
 */
public final class CombineTransform<T> implements Transform<T, T> {
  private static final Logger LOG = LoggerFactory.getLogger(CombineTransform.class.getName());

  private final Combine.CombineFn<T, T, T> combineFn;
  private OutputCollector<T> outputCollector;

  public CombineTransform(final Combine.CombineFn<T, T, T> combineFn) {
    this.combineFn = combineFn;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Iterator<T> elements, final String srcVertexId) {
    final List<T> iterable = new ArrayList<>();
    elements.forEachRemaining(element -> {
      iterable.add(element);
      LOG.info("CombineTransform onData: adding {}", element);
    });

    if (iterable.isEmpty()) {
      return;
    }

    T res = combineFn.apply(iterable);
    if (res == null) { // nothing to be done.
      return;
    }
    LOG.info("CombineTransform onData: emitting {}", res);
    outputCollector.emit(res);
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CombineTransform:");
    sb.append(super.toString());
    return sb.toString();
  }
}

