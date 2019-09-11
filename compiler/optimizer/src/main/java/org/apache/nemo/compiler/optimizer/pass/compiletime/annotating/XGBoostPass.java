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

package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.*;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionSetProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceAntiAffinityProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSiteProperty;
import org.apache.nemo.compiler.optimizer.OptimizerUtils;
import org.apache.nemo.runtime.common.metric.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Pass for applying XGBoost optimizations.
 * <p>
 * 1. The pass first triggers the client to run the XGBoost script, located under the `ml` python package.
 * 2. The client runs the script, which trains the tree model using the metrics collected before, and constructs
 *    a tree model, which then predicts the 'knobs' that minimizes the JCT based on the weights of the leaves (JCT).
 * 3. It receives the results, and in which direction each of the knobs should be optimized, and reconstructs the
 *    execution properties in the form that they are tuned.
 * 4. The newly reconstructed execution properties are injected and the workload runs after the optimization.
 */
@Annotates()
public final class XGBoostPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(XGBoostPass.class.getName());

  private static final BlockingQueue<String> MESSAGE_QUEUE = new LinkedBlockingQueue<>();

  /**
   * Default constructor.
   */
  public XGBoostPass() {
    super(XGBoostPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    try {
      final String message = XGBoostPass.takeMessage();
      LOG.info("Received message from the client: {}", message);

      if (message.isEmpty()) {
        LOG.info("No optimization included in the message. Returning the original dag.");
        return dag;
      } else {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, String>> listOfMap =
          mapper.readValue(message, new TypeReference<List<Map<String, String>>>() {
          });
        listOfMap.forEach(m -> {
          final String epKeyClass = m.get("EPKeyClass");
          final String epValueClass = m.get("EPValueClass");
          final String epValue = m.get("EPValue");
          if (!epValueClass.contains("ScheduleGroupProperty")) {
            final List<Object> objectList = OptimizerUtils.getObjects(m.get("type"), m.get("ID"), dag);
            LOG.info("Tuning: tuning with {} of {} for {} with {} with {}", m.get("type"), m.get("ID"),
              m.get("EPKeyClass"), m.get("EPValueClass"), m.get("EPValue"));
            ExecutionProperty<? extends Serializable> newEP = MetricUtils.keyAndValueToEP(
              epKeyClass, epValueClass, epValue);

            // Check the new EP if it is applicable.
            if (newEP.getClass().isAssignableFrom(ParallelismProperty.class)) {
              for (final Object o : objectList) {
                final Integer val = (Integer) newEP.getValue();
                if (o instanceof IRVertex) {
                  final IRVertex v = (IRVertex) o;
                  final Integer oneToOneParallelismVal = dag.getIncomingEdgesOf(v).stream()
                    .filter(e -> CommunicationPatternProperty.Value.ONE_TO_ONE
                      .equals(e.getPropertyValue(CommunicationPatternProperty.class).orElse(null)))
                    .mapToInt(e -> e.getSrc().getPropertyValue(ParallelismProperty.class).orElse(0))
                    .max().orElse(0);  // max value from one to one in edges.
                  final Integer shuffleParallelismVal = dag.getIncomingEdgesOf(v).stream()
                    .filter(e -> CommunicationPatternProperty.Value.SHUFFLE
                      .equals(e.getPropertyValue(CommunicationPatternProperty.class).orElse(null)))
                    .mapToInt(e -> e.getSrc().getPropertyValue(ParallelismProperty.class).orElse(0))
                    .max().orElse(0);  // max value from shuffle in edges.
                  final Integer parallelism = oneToOneParallelismVal > shuffleParallelismVal
                    ? oneToOneParallelismVal : shuffleParallelismVal;  // We get the larger value.
                  if (val > parallelism && parallelism > 0) {  // we set the maximum possible value.
                    newEP = ParallelismProperty.of(parallelism);
                  }
                }
              }
            } else if (newEP.getClass().isAssignableFrom(ResourceSiteProperty.class)) {
              for (final Object o : objectList) {
                if (o instanceof IRVertex) {
                  final IRVertex v = (IRVertex) o;
                  final List<HashMap<String, Integer>> oneToOneVal = dag.getIncomingEdgesOf(v).stream()
                    .filter(e -> CommunicationPatternProperty.Value.ONE_TO_ONE
                      .equals(e.getPropertyValue(CommunicationPatternProperty.class).orElse(null)))
                    .map(e -> e.getSrc().getPropertyValue(ResourceSiteProperty.class).orElse(new HashMap<>()))
                    .collect(Collectors.toList());
                  if (oneToOneVal.size() == 1) {
                    // we reset the resource site property for one-to-one relations.
                    newEP = ResourceSiteProperty.of(oneToOneVal.get(0));
                  }
                }
              }
            } else if (newEP.getClass().isAssignableFrom(ResourceAntiAffinityProperty.class)) {
              for (final Object o : objectList) {
                final HashSet<Integer> val = (HashSet<Integer>) newEP.getValue();
                if (o instanceof IRVertex) {
                  final IRVertex v = (IRVertex) o;
                  final Integer upperLimitOfTaskIndex =  // we limit the task index to the maximum parallelism value.
                    v.getPropertyValue(ParallelismProperty.class).orElse(Integer.MAX_VALUE);
                  final HashSet<Integer> newVal = new HashSet<>();

                  for (final Integer i : val) {
                    if (i < upperLimitOfTaskIndex) {
                      newVal.add(i);  // it's ok if it doesn't exceed the maximum parallelism value.
                    }
                  }
                  if (newVal.size() < val.size()) {
                    // if the two values don't match, we exclude those exceeding the max parallelism value.
                    newEP = ResourceAntiAffinityProperty.of(newVal);
                  }
                }
              }
            } else if (newEP.getClass().isAssignableFrom(PartitionerProperty.class)) {
              for (final Object o : objectList) {
                final Pair<PartitionerProperty.Type, Integer> val =
                  (Pair<PartitionerProperty.Type, Integer>) newEP.getValue();
                if (o instanceof IREdge) {
                  final IREdge e = (IREdge) o;
                  if (val.left() == PartitionerProperty.Type.HASH && !CommunicationPatternProperty.Value.SHUFFLE
                    .equals(e.getPropertyValue(CommunicationPatternProperty.class)
                      .orElse(CommunicationPatternProperty.Value.ONE_TO_ONE))) {
                    newEP = PartitionerProperty.of(PartitionerProperty.Type.INTACT);  // if HASH && not SHUFFLE, INTACT.
                  }
                }
              }
            } else if (newEP.getClass().isAssignableFrom(PartitionSetProperty.class)) {
              for (final Object o : objectList) {
                final ArrayList<KeyRange> val = (ArrayList<KeyRange>) newEP.getValue();
                if (o instanceof IREdge) {
                  final IREdge e = (IREdge) o;
                  if (val.size() > e.getDst().getPropertyValue(ParallelismProperty.class).orElse(0)) {
                    newEP = null;  // ignore if the key range list exceeds the parallelism of the destination.
                  }
                }
              }
            }

            try {
              MetricUtils.applyNewEPs(objectList, newEP, dag);
            } catch (IllegalVertexOperationException | IllegalEdgeOperationException e) {
            }
          }
        });
      }
    } catch (final InvalidParameterException e) {
      LOG.warn(e.getMessage());
      return dag;
    } catch (final Exception e) {
      throw new CompileTimeOptimizationException(e);
    }

    return dag;
  }

  /**
   * @param message push the message to the message queue.
   */
  public static void pushMessage(final String message) {
    MESSAGE_QUEUE.add(message);
  }

  /**
   * @return the message from the blocking queue.
   */
  private static String takeMessage() {
    try {
      return MESSAGE_QUEUE.take();
    } catch (InterruptedException e) {
      throw new MetricException("Interrupted while waiting for message: " + e);
    }
  }
}
