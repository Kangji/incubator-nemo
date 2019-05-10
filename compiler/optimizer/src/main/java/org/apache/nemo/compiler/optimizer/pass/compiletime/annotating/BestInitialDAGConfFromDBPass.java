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
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.runtime.common.metric.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Pass for applying the best existing execution properties & DAG from the DB.
 */
@Annotates()
public final class BestInitialDAGConfFromDBPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(BestInitialDAGConfFromDBPass.class.getName());

  public BestInitialDAGConfFromDBPass() {
    super(BestInitialDAGConfFromDBPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    try (Connection c = DriverManager.getConnection(MetricUtils.POSTGRESQL_METADATA_DB_NAME,
      "postgres", "fake_password")) {
      try (Statement statement = c.createStatement()) {
        statement.setQueryTimeout(30);  // set timeout to 30sec.

        try (ResultSet rs = statement.executeQuery("SELECT dag FROM " + dag.irDAGSummary()
          + " WHERE duration = (SELECT MIN (duration) FROM " + dag.irDAGSummary() + ")")) {
          LOG.info("Configuration loaded from DB");
          if (rs.next()) {
            return SerializationUtils.deserialize(rs.getBytes("dag"));  // best dag.
          }
        }
      }
    } catch (SQLException e) {
      throw new MetricException(e);
    }
    return dag;
  }
}
