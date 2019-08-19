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
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.compiler.optimizer.OptimizerUtils;
import org.apache.nemo.runtime.common.metric.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.util.List;
import java.util.Map;

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
    try (Connection c = DriverManager.getConnection(MetricUtils.POSTGRESQL_DB_NAME,
      "postgres", "fake_password")) {
      try (Statement statement = c.createStatement()) {
        statement.setQueryTimeout(30);  // set timeout to 30sec.

        try (ResultSet rs = statement.executeQuery("SELECT properties FROM nemo_data"
          + " WHERE duration = (SELECT MIN (duration) FROM nemo_data)")) {
          LOG.info("Configuration loaded from DB");
          if (rs.next()) {
            final ObjectMapper mapper = new ObjectMapper();
            final Map<String, Object> jsonObject = mapper.readValue(rs.getString("properties"),
              new TypeReference<Map<String, Object>>() {
              });

            final String type = (String) jsonObject.get("type");
            jsonObject.forEach((key, value) -> {
              LOG.info("key:" + key);
              if ("vertex".equals(key) || "edge".equals(key)) {
                ((List<Map<String, Object>>) value).forEach(e -> {
                  final List<Object> objectList = OptimizerUtils.getObjects(type, (String) e.get("ID"), dag);
                  final ExecutionProperty<? extends Serializable> newEP = MetricUtils.keyAndValueToEP(
                    (String) e.get("EPKeyClass"), (String) e.get("EPValueClass"), (String) e.get("EPValue"));
                  MetricUtils.applyNewEPs(objectList, newEP, dag);
                });
              }
            });
          }
        }
      }
    } catch (SQLException e) {
      throw new MetricException(e);
    } catch (IOException e) {
      throw new MetricException(e);
    }
    return dag;
  }
}
