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

package org.apache.nemo.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.*;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static org.apache.nemo.examples.beam.TPCHQueries.*;
import static org.apache.nemo.examples.beam.TPCDSQueries.*;

/**
 * TPC DS application.
 */
public final class TPC {
  /**
   * LOGGER.
   */
  private static final Logger LOG = LoggerFactory.getLogger(TPC.class.getName());

  /**
   * Private constructor.
   */
  private TPC() {
  }

  /**
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String queries = args[0];
    final String inputFilePath = args[1];
    final String outputFilePath = args[2];

    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("TPC");
    final Pipeline p = Pipeline.create(options);

    final List<String> tables;
    if (inputFilePath.contains("tpcds") || inputFilePath.contains("tpc-ds")) {
      //TPC-DS
      LOG.info("This TPC workload is TPC-DS");
      tables = Arrays.asList("customer",
        "customer_demographics", "date_dim", "inventory", "item",
        "promotion", "catalog_sales", "web_sales", "store_sales");
      //Other schemas have to be added.
      // Arrays.asList("catalog_page", "catalog_returns", "customer", "customer_address",
      // "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
      // "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
      // "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
      // "time_dim", "web_page");
    } else {
      //TPC-H
      LOG.info("This TPC workload is TPC-H");
      tables = Arrays.asList("part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region");
    }

    final Map<String, PCollection<Row>> tableMap = setupTables(p, inputFilePath, tables);

    PCollectionTuple tableTuple = PCollectionTuple.empty(p);
    for (Map.Entry<String, PCollection<Row>> e: tableMap.entrySet()) {
      tableTuple = tableTuple.and(new TupleTag<>(e.getKey()), e.getValue());
    }

    for (String q : filterQueries(inputFilePath, queries)) {
      final PCollection<Row> res = tableTuple.apply(SqlTransform.query(q));
      GenericSourceSink.write(res.apply(MapElements.via(new SimpleFunction<Row, String>() {
        @Override
        public String apply(final Row input) {
          LOG.info("QUERY {} OUTPUT: {}", q, input.getValues());
          return input.getValues().toString();
        }
      })), outputFilePath);
    }

    p.run();
  }

  /**
   * Set up a map of schemas for the tables.
   * @param inputFilePath the input file path of the tables.
   * @return the map of schemas for the tables.
   */
  private static Map<String, Schema> setupSchema(final String inputFilePath) {
    final Map<String, Schema> result = new HashMap<>();

    if (inputFilePath.contains("tpcds") || inputFilePath.contains("tpc-ds")) {
      //PART OF TPC-DS
      result.put("date_dim", DATE_DIM_SCHEMA);
      result.put("store_sales", STORE_SALES_SCHEMA);
      result.put("item", ITEM_SCHEMA);
      result.put("inventory", INVENTORY_SCHEMA);
      result.put("catalog_sales", CATALOG_SALES_SCHEMA);
      result.put("customer", GET_CUSTOMER_DS_SCHEMA);
      result.put("promotion", PROMOTION_SCHEMA);
      result.put("customer_demographics", CUSTOMER_DEMOGRAPHICS_SCHEMA);
      result.put("web_sales", WEB_SALES_SCHEMA);

    } else {
      //COMPLETE TPC-H
      result.put("part", PART_SCHEMA);
      result.put("supplier", SUPPLIER_SCHEMA);
      result.put("partsupp", PARTSUPP_SCHEMA);
      result.put("customer", CUSTOMER_SCHEMA);
      result.put("orders", ORDER_SCHEMA);
      result.put("lineitem", LINEITEM_SCHEMA);
      result.put("nation", NATION_SCHEMA);
      result.put("region", REGION_SCHEMA);
    }

    return result;
  }

  /**
   * Set up tables to process.
   * @param p the pipeline to append the tables.
   * @param inputFilePath the input file path for the tables.
   * @param tables the list of tables to load.
   * @return map of table name to the actual table.
   */
  private static Map<String, PCollection<Row>> setupTables(final Pipeline p, final String inputFilePath,
                                                           final List<String> tables) {
    final Map<String, PCollection<Row>> result = new HashMap<>();
    final Map<String, Schema> schemaMap = setupSchema(inputFilePath);

    tables.forEach(t -> {
      final Schema tableSchema = schemaMap.get(t);
      final CSVFormat csvFormat;
      final String filePattern;
      if (inputFilePath.contains("tpcds") || inputFilePath.contains("tpc-ds")) {
        csvFormat = CSVFormat.DEFAULT.withNullString("");
        filePattern = inputFilePath + "/" + t + "/*.csv";
      } else {
        csvFormat = CSVFormat.DEFAULT.withDelimiter('|').withTrailingDelimiter().withNullString("");
        filePattern = inputFilePath + "/" + t + ".tbl";
      }

      final PCollection<Row> table =
        GenericSourceSink.read(p, filePattern)
          .apply("StringToRow", new TextTableProvider.CsvToRow(tableSchema, csvFormat))
          .setName(t);

      result.put(t, table);
    });

    return result;
  }

  /**
   * Filtering out the required queries.
   * @param inputFilePath input file path to determine TPC-DS and TPC-H workloads.
   * @param queries queries to filter.
   * @return the filtered queries.
   */
  private static List<String> filterQueries(final String inputFilePath, final String queries) {
    final List<String> queryNames = Arrays.asList(queries.split(","));
    final Map<String, String> queryMap = new HashMap<>();
    if (inputFilePath.contains("tpcds") || inputFilePath.contains("tpc-ds")) {
      queryMap.put("q2", QUERY2);
      queryMap.put("q3", QUERY3);
      queryMap.put("q4", QUERY4);
      queryMap.put("q7", QUERY7);
      //      queryMap.put("q11", QUERY11);
      //      queryMap.put("q12", QUERY12);
      queryMap.put("q22", QUERY22);
      //      queryMap.put("q38", QUERY38);
      queryMap.put("q42", QUERY42);
      queryMap.put("q51", QUERY51);
      queryMap.put("q76", QUERY76);
      queryMap.put("q78", QUERY78);

    } else {
      queryMap.put("q1", TPCH_QUERY1);
      queryMap.put("q2", TPCH_QUERY2);
      queryMap.put("q3", TPCH_QUERY3);
      queryMap.put("q4", TPCH_QUERY4);
      queryMap.put("q5", TPCH_QUERY5);
      queryMap.put("q6", TPCH_QUERY6);
      queryMap.put("q7", TPCH_QUERY7);
      queryMap.put("q8", TPCH_QUERY8);
      queryMap.put("q9", TPCH_QUERY9);
      queryMap.put("q10", TPCH_QUERY10);
      queryMap.put("q11", TPCH_QUERY11);
      queryMap.put("q12", TPCH_QUERY12);
      queryMap.put("q13", TPCH_QUERY13);
      queryMap.put("q14", TPCH_QUERY14);
      queryMap.put("q15", TPCH_QUERY15);
      queryMap.put("q16", TPCH_QUERY16);
      queryMap.put("q17", TPCH_QUERY17);
      queryMap.put("q18", TPCH_QUERY18);
      queryMap.put("q19", TPCH_QUERY19);
      queryMap.put("q20", TPCH_QUERY20);
      queryMap.put("q21", TPCH_QUERY21);
      queryMap.put("q22", TPCH_QUERY22);
    }

    final List<String> res = new ArrayList<>();
    queryNames.forEach(n -> {
      final String query = queryMap.get(n.trim().toLowerCase());
      if (query != null) {
        res.add(query);
      } else {
        res.add(getQueryString(n));
      }
    });
    return res;
  }


  /**
   * Get query string.
   * @param queryFilePath file path to the query.
   * @return the query string.
   */
  private static String getQueryString(final String queryFilePath) {
    final List<String> lines = new ArrayList<>();
    try (Stream<String> stream  = Files.lines(Paths.get(queryFilePath))) {
      stream.forEach(lines::add);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    LOG.info("QUERY LINES: {}", lines);

    final StringBuilder sb = new StringBuilder();
    lines.forEach(line -> {
      sb.append(" ");
      sb.append(line);
    });

    final String concate = sb.toString();
    LOG.info("QUERY CONCAT: {}", concate);
    final String cleanOne = concate.replaceAll("\n", " ");
    LOG.info("QUERY CLEAN1: {}", cleanOne);
    final String cleanTwo = cleanOne.replaceAll("\t", " ");
    LOG.info("QUERY CLEAN2: {}", cleanTwo);

    return cleanTwo;
  }

}
