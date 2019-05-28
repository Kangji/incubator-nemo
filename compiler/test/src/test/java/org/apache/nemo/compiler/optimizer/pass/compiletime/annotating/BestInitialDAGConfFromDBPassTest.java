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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * Test {@link BestInitialDAGConfFromDBPass}.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.apache.http.conn.ssl.*", "javax.net.ssl.*" , "javax.crypto.*"})
@PrepareForTest(JobLauncher.class)
public class BestInitialDAGConfFromDBPassTest {
  private static final Logger LOG = LoggerFactory.getLogger(BestInitialDAGConfFromDBPassTest.class.getName());

  private IRDAG compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileWordCountDAG();
  }

  @Test
  public void testMatchingDAGs() {
    final IRDAG originalDAG = SerializationUtils.deserialize(SerializationUtils.serialize(compiledDAG));
    final IRDAG processedDAG = new BestInitialDAGConfFromDBPass().apply(compiledDAG);

    final Iterator<IRVertex> originalVertices = originalDAG.getTopologicalSort().iterator();
    final Iterator<IRVertex> processedVertices = processedDAG.getTopologicalSort().iterator();

    while (originalVertices.hasNext() && processedVertices.hasNext()) {
      final IRVertex processedVertex = processedVertices.next();
      if (processedVertex.isUtilityVertex()) {
        LOG.info("Utility vertex {}", processedVertex);
      } else {
        final IRVertex originalVertex = originalVertices.next();

        assertEquals(originalVertex.toString(), processedVertex.toString());
      }
    }

    while (processedVertices.hasNext() && processedVertices.next().isUtilityVertex()) {
      continue;
    }

    assertFalse(originalVertices.hasNext());
    assertFalse(processedVertices.hasNext());
  }
}
