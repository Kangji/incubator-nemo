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

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.JsonParseException;
import org.apache.nemo.common.exception.UnsupportedMethodException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.runtime.common.metric.MetricUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
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

  /**
   * Utility method for parsing the resource specification string.
   *
   * @param resourceSpecificationString the input resource specification string.
   * @return the parsed list of resource specifications. Each element consists of a pair of 'resource type' and a list
   * containing 'memory', 'capacity', 'number of executors', and 'seconds of poisoning', in the order specified.
   */
  public static List<Pair<String, List<Integer>>> parseResourceSpecificationString(
    final String resourceSpecificationString) {
    final List<Pair<String, List<Integer>>> resourceSpecifications = new ArrayList<>();
    try {
      if (resourceSpecificationString.trim().startsWith("[")) {
        final TreeNode jsonRootNode = new ObjectMapper().readTree(resourceSpecificationString);

        for (int i = 0; i < jsonRootNode.size(); i++) {
          final TreeNode resourceNode = jsonRootNode.get(i);
          final String type = resourceNode.get("type").traverse().nextTextValue();
          final int memory = resourceNode.get("memory_mb").traverse().getIntValue();
          final int capacity = resourceNode.get("capacity").traverse().getIntValue();
          final int executorNum = resourceNode.path("num").traverse().nextIntValue(1);
          final int poisonSec = resourceNode.path("poison_sec").traverse().nextIntValue(-1);

          final List<Integer> specs = Arrays.asList(memory, capacity, executorNum, poisonSec);
          resourceSpecifications.add(Pair.of(type, specs));
        }
      } else if (resourceSpecificationString.trim().startsWith("<")) {
        final InputSource is = new InputSource(new StringReader(resourceSpecificationString));
        final DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        final Document document = docBuilder.parse(is);
        final Element root = document.getDocumentElement();

        final NodeList clusters = root.getElementsByTagName("cluster");
        for (int i = 0; i < clusters.getLength(); i++) {
          final Node cluster = clusters.item(i);
          final NodeList nodes = cluster.getChildNodes();
          for (int j = 0; j < nodes.getLength(); j++) {
            final Node node = nodes.item(j);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
              final Element n = ((Element) node);
              final String type = n.getElementsByTagName("type").item(0).getTextContent();
              final int memory = Integer.parseInt(n.getElementsByTagName("memory_mb").item(0).getTextContent());
              final int capacity = Integer.parseInt(n.getElementsByTagName("capacity").item(0).getTextContent());
              final int executorNum = n.getElementsByTagName("num").item(0) == null ? 1  // default
                : Integer.parseInt(n.getElementsByTagName("num").item(0).getTextContent());
              final int poisonSec = n.getElementsByTagName("poison_sec").item(0) == null ? -1  // default
                : Integer.parseInt(n.getElementsByTagName("poison_sec").item(0).getTextContent());

              final List<Integer> specs = Arrays.asList(memory, capacity, executorNum, poisonSec);
              resourceSpecifications.add(Pair.of(type, specs));
            }
          }
        }
      } else {
        throw new UnsupportedOperationException("Executor Info file should be a JSON or an XML file.");
      }

      return resourceSpecifications;
    } catch (final Exception e) {
      throw new JsonParseException(e);
    }
  }
}
