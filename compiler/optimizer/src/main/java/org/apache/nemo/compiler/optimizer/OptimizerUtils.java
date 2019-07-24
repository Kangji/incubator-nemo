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

package org.apache.nemo.compiler.optimizer;

import org.apache.nemo.common.exception.UnsupportedMethodException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.runtime.common.metric.MetricUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for optimizer.
 */
public final class OptimizerUtils {

  /**
   * Private constructor.
   */
  private OptimizerUtils() {
  }

  /**
   * Get the list of objects to tune from the type, id, and the DAG.
   * @param type the type of the optimization: pattern or id.
   * @param id   the identifier for the pattern of the id of the component.
   * @param dag  the DAG to find the component of.
   *
   * @return the list of objects from the identifier.
   */
  public static List<Object> getObjects(final String type, final String id, final IRDAG dag) {
    final List<Object> objectList = new ArrayList<>();
    if (type.equals("pattern")) {
      objectList.addAll(MetricUtils.getObjectFromPatternString(id, dag));
    } else if (type.equals("id")) {
      if (id.startsWith("vertex")) {
        objectList.add(dag.getVertexById(id));
      } else if (id.startsWith("edge")) {
        objectList.add(dag.getEdgeById(id));
      } else {
        throw new UnsupportedMethodException("The id " + id + " cannot be categorized into a vertex or an edge");
      }
    }

    return objectList;
  }

  /**
   * Method to infiltrate keyword-containing string into the enum of Types above.
   *
   * @param environmentType the input string.
   * @return the formatted string corresponding to each type.
   */
  public static String filterEnvironmentTypeString(final String environmentType) {
    if (environmentType.toLowerCase().contains("transient")) {
      return "transient";
    } else if (environmentType.toLowerCase().contains("large") && environmentType.toLowerCase().contains("shuffle")) {
      return "large_shuffle";
    } else if (environmentType.toLowerCase().contains("disaggregat")) {
      return "disaggregation";
    } else if (environmentType.toLowerCase().contains("stream")) {
      return "streaming";
    } else if (environmentType.toLowerCase().contains("small")) {
      return "small_size";
    } else if (environmentType.toLowerCase().contains("skew")) {
      return "data_skew";
    } else {
      return "";  // Default
    }
  }
}
