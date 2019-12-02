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

    for (String q : filterQueries(queries)) {
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
      result.put("date_dim", dateDimSchema);
      result.put("store_sales", storeSalesSchema);
      result.put("item", itemSchema);
      result.put("inventory", inventorySchema);
      result.put("catalog_sales", catalogSalesSchema);
      result.put("customer", getCustomerDsSchema);
      result.put("promotion", promotionSchema);
      result.put("customer_demographics", customerDemographicsSchema);
      result.put("web_sales", webSalesSchema);

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
      final CSVFormat csvFormat = CSVFormat.DEFAULT.withNullString("");
      final String filePattern = inputFilePath + "/" + t + "/*.csv";

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
   * @param queries queries to filter.
   * @return the filtered queries.
   */
  private static List<String> filterQueries(final String queries) {
    final List<String> queryNames = Arrays.asList(queries.split(","));
    final Map<String, String> queryMap = new HashMap<>();
    queryMap.put("q2", QUERY2);
    queryMap.put("q3", QUERY3);
    queryMap.put("q4", QUERY4);
    queryMap.put("q7", QUERY7);
//    queryMap.put("q11", QUERY11);
//    queryMap.put("q12", QUERY12);
    queryMap.put("q22", QUERY22);
//    queryMap.put("q38", QUERY38);
    queryMap.put("q42", QUERY42);
    queryMap.put("q51", QUERY51);
    queryMap.put("q76", QUERY76);
    queryMap.put("q78", QUERY78);

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

  /**
   * TPC-DS Schemas.
   */

  private static Schema storeSalesSchema =
    Schema.builder()
      .addNullableField("ss_sold_date_sk", Schema.FieldType.INT32)
      .addNullableField("ss_sold_time_sk", Schema.FieldType.INT32)
      .addNullableField("ss_item_sk", Schema.FieldType.INT32)
      .addNullableField("ss_customer_sk", Schema.FieldType.STRING)
      .addNullableField("ss_cdemo_sk", Schema.FieldType.INT32)
      .addNullableField("ss_hdemo_sk", Schema.FieldType.INT32)
      .addNullableField("ss_addr_sk", Schema.FieldType.INT32)
      .addNullableField("ss_store_sk", Schema.FieldType.INT32)
      .addNullableField("ss_promo_sk", Schema.FieldType.INT32)
      .addNullableField("ss_ticket_number", Schema.FieldType.INT64)
      .addNullableField("ss_quantity", Schema.FieldType.INT32)
      .addNullableField("ss_wholesale_cost", Schema.FieldType.FLOAT)
      .addNullableField("ss_list_price", Schema.FieldType.FLOAT)
      .addNullableField("ss_sales_price", Schema.FieldType.FLOAT)
      .addNullableField("ss_ext_discount_amt", Schema.FieldType.FLOAT)
      .addNullableField("ss_ext_sales_price", Schema.FieldType.FLOAT)
      .addNullableField("ss_ext_wholesale_cost", Schema.FieldType.FLOAT)
      .addNullableField("ss_ext_list_price", Schema.FieldType.FLOAT)
      .addNullableField("ss_ext_tax", Schema.FieldType.FLOAT)
      .addNullableField("ss_coupon_amt", Schema.FieldType.FLOAT)
      .addNullableField("ss_net_paid", Schema.FieldType.FLOAT)
      .addNullableField("ss_net_paid_inc_tax", Schema.FieldType.FLOAT)
      .addNullableField("ss_net_profit", Schema.FieldType.FLOAT)
      .build();


  private static Schema dateDimSchema =
    Schema.builder()
      .addNullableField("d_date_sk", Schema.FieldType.INT32)
      .addNullableField("d_date_id", Schema.FieldType.STRING)
      .addNullableField("d_date", Schema.FieldType.STRING)
      .addNullableField("d_month_seq", Schema.FieldType.INT32)
      .addNullableField("d_week_seq", Schema.FieldType.INT32)
      .addNullableField("d_quarter_seq", Schema.FieldType.INT32)
      .addNullableField("d_year", Schema.FieldType.INT32)
      .addNullableField("d_dow", Schema.FieldType.INT32)
      .addNullableField("d_moy", Schema.FieldType.INT32)
      .addNullableField("d_dom", Schema.FieldType.INT32)
      .addNullableField("d_qoy", Schema.FieldType.INT32)
      .addNullableField("d_fy_year", Schema.FieldType.INT32)
      .addNullableField("d_fy_quarter_seq", Schema.FieldType.INT32)
      .addNullableField("d_fy_week_seq", Schema.FieldType.INT32)
      .addNullableField("d_day_name", Schema.FieldType.STRING)
      .addNullableField("d_quarter_name", Schema.FieldType.STRING)
      .addNullableField("d_holiday", Schema.FieldType.STRING)
      .addNullableField("d_weekend", Schema.FieldType.STRING)
      .addNullableField("d_following_holiday", Schema.FieldType.STRING)
      .addNullableField("d_first_dom", Schema.FieldType.INT32)
      .addNullableField("d_last_dom", Schema.FieldType.INT32)
      .addNullableField("d_same_day_ly", Schema.FieldType.INT32)
      .addNullableField("d_same_day_lq", Schema.FieldType.INT32)
      .addNullableField("d_current_day", Schema.FieldType.STRING)
      .addNullableField("d_current_week", Schema.FieldType.STRING)
      .addNullableField("d_current_month", Schema.FieldType.STRING)
      .addNullableField("d_current_quarter", Schema.FieldType.STRING)
      .addNullableField("d_current_year", Schema.FieldType.STRING)
      .build();

  private static Schema itemSchema =
    Schema.builder()
      .addNullableField("i_item_sk", Schema.FieldType.INT32)
      .addNullableField("i_item_id", Schema.FieldType.STRING) //                 .string,
      .addNullableField("i_rec_start_date", Schema.FieldType.DATETIME) //          .date,
      .addNullableField("i_rec_end_date", Schema.FieldType.DATETIME) //            .date,
      .addNullableField("i_item_desc", Schema.FieldType.STRING) //             .string,
      .addNullableField("i_current_price", Schema.FieldType.FLOAT) //           .decimal(7,2),
      .addNullableField(
        "i_wholesale_cost", Schema.FieldType.FLOAT) //               .decimal(7,2),
      .addNullableField("i_brand_id", Schema.FieldType.INT32) //                .int,
      .addNullableField("i_brand", Schema.FieldType.STRING) //                   .string,
      .addNullableField("i_class_id", Schema.FieldType.INT32) //                .int,
      .addNullableField("i_class", Schema.FieldType.STRING) //                   .string,
      .addNullableField("i_category_id", Schema.FieldType.INT32) //             .int,
      .addNullableField("i_category", Schema.FieldType.STRING) //       .string,
      .addNullableField("i_manufact_id", Schema.FieldType.INT32) //             .int,
      .addNullableField("i_manufact", Schema.FieldType.STRING) //                .string,
      .addNullableField("i_size", Schema.FieldType.STRING) //                    .string,
      .addNullableField("i_formulation", Schema.FieldType.STRING) //             .string,
      .addNullableField("i_color", Schema.FieldType.STRING) //                   .string,
      .addNullableField("i_units", Schema.FieldType.STRING) //                   .string,
      .addNullableField("i_container", Schema.FieldType.STRING) //               .string,
      .addNullableField("i_manager_id", Schema.FieldType.INT32) // .int,
      .addNullableField("i_product_name", Schema.FieldType.STRING) //            .string),
      .build();

  private static Schema inventorySchema =
    Schema.builder()
      .addNullableField("inv_date_sk", Schema.FieldType.INT32)
      .addNullableField("inv_item_sk", Schema.FieldType.INT32)
      .addNullableField("inv_warehouse_sk", Schema.FieldType.INT32)
      .addNullableField("inv_quantity_on_hand", Schema.FieldType.INT32)
      .build();

  private static Schema catalogSalesSchema =
    Schema.builder()
      .addNullableField("cs_sold_date_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("cs_sold_time_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("cs_ship_date_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("cs_bill_customer_sk", Schema.FieldType.INT32) //      .int,
      .addNullableField("cs_bill_cdemo_sk", Schema.FieldType.INT32) //         .int,
      .addNullableField("cs_bill_hdemo_sk", Schema.FieldType.INT32) //         .int,
      .addNullableField("cs_bill_addr_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("cs_ship_customer_sk", Schema.FieldType.INT32) //      .int,
      .addNullableField("cs_ship_cdemo_sk", Schema.FieldType.INT32) //         .int,
      .addNullableField("cs_ship_hdemo_sk", Schema.FieldType.INT32) //         .int,
      .addNullableField("cs_ship_addr_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("cs_call_center_sk", Schema.FieldType.INT32) //        .int,
      .addNullableField("cs_catalog_page_sk", Schema.FieldType.INT32) //       .int,
      .addNullableField("cs_ship_mode_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("cs_warehouse_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("cs_item_sk", Schema.FieldType.INT32) //               .int,
      .addNullableField("cs_promo_sk", Schema.FieldType.INT32) //              .int,
      .addNullableField("cs_order_number", Schema.FieldType.INT64) //          .long,
      .addNullableField("cs_quantity", Schema.FieldType.INT32) //              .int,
      .addNullableField("cs_wholesale_cost", Schema.FieldType.FLOAT) //        .decimal(7,2),
      .addNullableField("cs_list_price", Schema.FieldType.FLOAT) //            .decimal(7,2),
      .addNullableField("cs_sales_price", Schema.FieldType.FLOAT) //           .decimal(7,2),
      .addNullableField("cs_ext_discount_amt", Schema.FieldType.FLOAT) //      .decimal(7,2),
      .addNullableField("cs_ext_sales_price", Schema.FieldType.FLOAT) //       .decimal(7,2),
      .addNullableField("cs_ext_wholesale_cost", Schema.FieldType.FLOAT) //    .decimal(7,2),
      .addNullableField("cs_ext_list_price", Schema.FieldType.FLOAT) //        .decimal(7,2),
      .addNullableField("cs_ext_tax", Schema.FieldType.FLOAT) //               .decimal(7,2),
      .addNullableField("cs_coupon_amt", Schema.FieldType.FLOAT) //            .decimal(7,2),
      .addNullableField("cs_ext_ship_cost", Schema.FieldType.FLOAT) //         .decimal(7,2),
      .addNullableField("cs_net_paid", Schema.FieldType.FLOAT) //              .decimal(7,2),
      .addNullableField("cs_net_paid_inc_tax", Schema.FieldType.FLOAT) //      .decimal(7,2),
      .addNullableField("cs_net_paid_inc_ship", Schema.FieldType.FLOAT) //     .decimal(7,2),
      .addNullableField("cs_net_paid_inc_ship_tax", Schema.FieldType.FLOAT) // .decimal(7,2),
      .addNullableField("cs_net_profit", Schema.FieldType.FLOAT) //            .decimal(7,2))
      .build();


  private static Schema getCustomerDsSchema =
    Schema.builder()
      .addNullableField("c_customer_sk", Schema.FieldType.INT32) //             .int,
      .addNullableField("c_customer_id", Schema.FieldType.STRING) //             .string,
      .addNullableField("c_current_cdemo_sk", Schema.FieldType.INT32) //        .int,
      .addNullableField("c_current_hdemo_sk", Schema.FieldType.INT32) //        .int,
      .addNullableField("c_current_addr_sk", Schema.FieldType.INT32) //         .int,
      .addNullableField("c_first_shipto_date_sk", Schema.FieldType.INT32) //    .int,
      .addNullableField("c_first_sales_date_sk", Schema.FieldType.INT32) //     .int,
      .addNullableField("c_salutation", Schema.FieldType.STRING) //              .string,
      .addNullableField("c_first_name", Schema.FieldType.STRING) //              .string,
      .addNullableField("c_last_name", Schema.FieldType.STRING) //               .string,
      .addNullableField("c_preferred_cust_flag", Schema.FieldType.STRING) //     .string,
      .addNullableField("c_birth_day", Schema.FieldType.INT32) //               .int,
      .addNullableField("c_birth_month", Schema.FieldType.INT32) //             .int,
      .addNullableField("c_birth_year", Schema.FieldType.INT32) //              .int,
      .addNullableField("c_birth_country", Schema.FieldType.STRING) //           .string,
      .addNullableField("c_login", Schema.FieldType.STRING) //                   .string,
      .addNullableField("c_email_address", Schema.FieldType.STRING) //           .string,
      .addNullableField("c_last_review_date", Schema.FieldType.STRING) //        .string)
      .build();


  private static Schema promotionSchema =
    Schema.builder()
      .addNullableField("p_promo_sk", Schema.FieldType.INT32)
      .addNullableField("p_promo_id", Schema.FieldType.STRING) //                .string,
      .addNullableField("p_start_date_sk", Schema.FieldType.INT32) // .int,
      .addNullableField("p_end_date_sk", Schema.FieldType.INT32) // .int,
      .addNullableField("p_item_sk", Schema.FieldType.INT32) // .int,
      .addNullableField("p_cost", Schema.FieldType.FLOAT) // .decimal(15,2),
      .addNullableField("p_response_target", Schema.FieldType.INT32) // .int,
      .addNullableField("p_promo_name", Schema.FieldType.STRING) // .string,
      .addNullableField("p_channel_dmail", Schema.FieldType.STRING) // .string,
      .addNullableField("p_channel_email", Schema.FieldType.STRING) // .string,
      .addNullableField("p_channel_catalog", Schema.FieldType.STRING) // .string,
      .addNullableField("p_channel_tv", Schema.FieldType.STRING) // .string,
      .addNullableField("p_channel_radio", Schema.FieldType.STRING) // .string,
      .addNullableField("p_channel_press", Schema.FieldType.STRING) // .string,
      .addNullableField("p_channel_event", Schema.FieldType.STRING) // .string,
      .addNullableField("p_channel_demo", Schema.FieldType.STRING) // .string,
      .addNullableField("p_channel_details", Schema.FieldType.STRING) // .string,
      .addNullableField("p_purpose", Schema.FieldType.STRING) // .string,
      .addNullableField("p_discount_active", Schema.FieldType.STRING) // .string),
      .build();

  private static Schema customerDemographicsSchema =
    Schema.builder()
      .addNullableField("cd_demo_sk", Schema.FieldType.INT32)
      .addNullableField("cd_gender", Schema.FieldType.STRING) //                 .string,
      .addNullableField("cd_marital_status", Schema.FieldType.STRING) //         .string,
      .addNullableField("cd_education_status", Schema.FieldType.STRING) //       .string,
      .addNullableField("cd_purchase_estimate", Schema.FieldType.INT32) //      .int,
      .addNullableField("cd_credit_rating", Schema.FieldType.STRING) //          .string,
      .addNullableField("cd_dep_count", Schema.FieldType.INT32) //              .int,
      .addNullableField("cd_dep_employed_count", Schema.FieldType.INT32) //     .int,
      .addNullableField("cd_dep_college_count", Schema.FieldType.INT32) //      .int),
      .build();

  private static Schema webSalesSchema =
    Schema.builder()
      .addNullableField("ws_sold_date_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("ws_sold_time_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("ws_ship_date_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("ws_item_sk", Schema.FieldType.INT32) //               .int,
      .addNullableField("ws_bill_customer_sk", Schema.FieldType.INT32) //      .int,
      .addNullableField("ws_bill_cdemo_sk", Schema.FieldType.INT32) //         .int,
      .addNullableField("ws_bill_hdemo_sk", Schema.FieldType.INT32) //         .int,
      .addNullableField("ws_bill_addr_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("ws_ship_customer_sk", Schema.FieldType.INT32) //      .int,
      .addNullableField("ws_ship_cdemo_sk", Schema.FieldType.INT32) //         .int,
      .addNullableField("ws_ship_hdemo_sk", Schema.FieldType.INT32) //         .int,
      .addNullableField("ws_ship_addr_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("ws_web_page_sk", Schema.FieldType.INT32) //           .int,
      .addNullableField("ws_web_site_sk", Schema.FieldType.INT32) //           .int,
      .addNullableField("ws_ship_mode_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("ws_warehouse_sk", Schema.FieldType.INT32) //          .int,
      .addNullableField("ws_promo_sk", Schema.FieldType.INT32) //              .int,
      .addNullableField("ws_order_number", Schema.FieldType.INT64) //          .long,
      .addNullableField("ws_quantity", Schema.FieldType.INT32) //              .int,
      .addNullableField("ws_wholesale_cost", Schema.FieldType.FLOAT) //        .decimal(7,2),
      .addNullableField("ws_list_price", Schema.FieldType.FLOAT) //            .decimal(7,2),
      .addNullableField("ws_sales_price", Schema.FieldType.FLOAT) //           .decimal(7,2),
      .addNullableField("ws_ext_discount_amt", Schema.FieldType.FLOAT) //      .decimal(7,2),
      .addNullableField("ws_ext_sales_price", Schema.FieldType.FLOAT) //       .decimal(7,2),
      .addNullableField("ws_ext_wholesale_cost", Schema.FieldType.FLOAT) //    .decimal(7,2),
      .addNullableField("ws_ext_list_price", Schema.FieldType.FLOAT) //        .decimal(7,2),
      .addNullableField("ws_ext_tax", Schema.FieldType.FLOAT) //               .decimal(7,2),
      .addNullableField("ws_coupon_amt", Schema.FieldType.FLOAT) //            .decimal(7,2),
      .addNullableField("ws_ext_ship_cost", Schema.FieldType.FLOAT) //         .decimal(7,2),
      .addNullableField("ws_net_paid", Schema.FieldType.FLOAT) //              .decimal(7,2),
      .addNullableField("ws_net_paid_inc_tax", Schema.FieldType.FLOAT) //      .decimal(7,2),
      .addNullableField("ws_net_paid_inc_ship", Schema.FieldType.FLOAT) //     .decimal(7,2),
      .addNullableField("ws_net_paid_inc_ship_tax", Schema.FieldType.FLOAT) // .decimal(7,2),
      .addNullableField("ws_net_profit", Schema.FieldType.FLOAT) //            .decimal(7,2)),
      .build();

  public static final String QUERY2 =
    "with wscs as\n"
      + "  (select sold_date_sk\n"
      + "         ,sales_price\n"
      + "   from  select ws_sold_date_sk sold_date_sk\n"
      + "               ,ws_ext_sales_price sales_price\n"
      + "         from web_sales\n"
      + "         union all\n"
      + "         select cs_sold_date_sk sold_date_sk\n"
      + "               ,cs_ext_sales_price sales_price\n"
      + "         from catalog_sales),\n"
      + "  wswscs as\n"
      + "  (select d_week_seq,\n"
      + "         sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,\n"
      + "         sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,\n"
      + "         sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,\n"
      + "         sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,\n"
      + "         sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,\n"
      + "         sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,\n"
      + "         sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales\n"
      + "  from wscs\n"
      + "      ,date_dim\n"
      + "  where d_date_sk = sold_date_sk\n"
      + "  group by d_week_seq)\n"
      + "  select d_week_seq1\n"
      + "        ,round(sun_sales1/sun_sales2,2)\n"
      + "        ,round(mon_sales1/mon_sales2,2)\n"
      + "        ,round(tue_sales1/tue_sales2,2)\n"
      + "        ,round(wed_sales1/wed_sales2,2)\n"
      + "        ,round(thu_sales1/thu_sales2,2)\n"
      + "        ,round(fri_sales1/fri_sales2,2)\n"
      + "        ,round(sat_sales1/sat_sales2,2)\n"
      + "  from\n"
      + "  (select wswscs.d_week_seq d_week_seq1\n"
      + "         ,sun_sales sun_sales1\n"
      + "         ,mon_sales mon_sales1\n"
      + "         ,tue_sales tue_sales1\n"
      + "         ,wed_sales wed_sales1\n"
      + "         ,thu_sales thu_sales1\n"
      + "         ,fri_sales fri_sales1\n"
      + "         ,sat_sales sat_sales1\n"
      + "   from wswscs,date_dim\n"
      + "   where date_dim.d_week_seq = wswscs.d_week_seq and\n"
      + "         d_year = 1998) y,\n"
      + "  (select wswscs.d_week_seq d_week_seq2\n"
      + "         ,sun_sales sun_sales2\n"
      + "         ,mon_sales mon_sales2\n"
      + "         ,tue_sales tue_sales2\n"
      + "         ,wed_sales wed_sales2\n"
      + "         ,thu_sales thu_sales2\n"
      + "         ,fri_sales fri_sales2\n"
      + "         ,sat_sales sat_sales2\n"
      + "   from wswscs\n"
      + "       ,date_dim\n"
      + "   where date_dim.d_week_seq = wswscs.d_week_seq and\n"
      + "         d_year = 1998+1) z\n"
      + "  where d_week_seq1=d_week_seq2-53\n"
      + "  order by d_week_seq1";

  public static final String QUERY3 =
    "select  dt.d_year \n"
      + "       ,item.i_brand_id brand_id \n"
      + "       ,item.i_brand brand\n"
      + "       ,sum(ss_sales_price) sum_agg\n"
      + " from  date_dim dt \n"
      + "      ,store_sales\n"
      + "      ,item\n"
      + " where dt.d_date_sk = store_sales.ss_sold_date_sk\n"
      + "   and store_sales.ss_item_sk = item.i_item_sk\n"
      + "   and item.i_manufact_id = 816\n"
      + "   and dt.d_moy=11\n"
      + " group by dt.d_year\n"
      + "      ,item.i_brand\n"
      + "      ,item.i_brand_id\n"
      + " order by dt.d_year\n"
      + "         ,sum_agg desc\n"
      + "         ,brand_id\n"
      + " limit 100";

  public static final String QUERY4 =
    "with year_total as (\n"
      + "  select c_customer_id customer_id\n"
      + "        ,c_first_name customer_first_name\n"
      + "        ,c_last_name customer_last_name\n"
      + "        ,c_preferred_cust_flag customer_preferred_cust_flag\n"
      + "        ,c_birth_country customer_birth_country\n"
      + "        ,c_login customer_login\n"
      + "        ,c_email_address customer_email_address\n"
      + "        ,d_year dyear\n"
      + "        ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total\n"
      + "        ,'s' sale_type\n"
      + "  from customer\n"
      + "      ,store_sales\n"
      + "      ,date_dim\n"
      + "  where c_customer_sk = ss_customer_sk\n"
      + "    and ss_sold_date_sk = d_date_sk\n"
      + "  group by c_customer_id\n"
      + "          ,c_first_name\n"
      + "          ,c_last_name\n"
      + "          ,c_preferred_cust_flag\n"
      + "          ,c_birth_country\n"
      + "          ,c_login\n"
      + "          ,c_email_address\n"
      + "          ,d_year\n"
      + "  union all\n"
      + "  select c_customer_id customer_id\n"
      + "        ,c_first_name customer_first_name\n"
      + "        ,c_last_name customer_last_name\n"
      + "        ,c_preferred_cust_flag customer_preferred_cust_flag\n"
      + "        ,c_birth_country customer_birth_country\n"
      + "        ,c_login customer_login\n"
      + "        ,c_email_address customer_email_address\n"
      + "        ,d_year dyear\n"
      + "        ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total\n"
      + "        ,'c' sale_type\n"
      + "  from customer\n"
      + "      ,catalog_sales\n"
      + "      ,date_dim\n"
      + "  where c_customer_sk = cs_bill_customer_sk\n"
      + "and cs_sold_date_sk = d_date_sk\n"
      + "  group by c_customer_id\n"
      + "          ,c_first_name\n"
      + "          ,c_last_name\n"
      + "          ,c_preferred_cust_flag\n"
      + "          ,c_birth_country\n"
      + "          ,c_login\n"
      + "          ,c_email_address\n"
      + "          ,d_year\n"
      + " union all\n"
      + "  select c_customer_id customer_id\n"
      + "        ,c_first_name customer_first_name\n"
      + "        ,c_last_name customer_last_name\n"
      + "        ,c_preferred_cust_flag customer_preferred_cust_flag\n"
      + "        ,c_birth_country customer_birth_country\n"
      + "        ,c_login customer_login\n"
      + "        ,c_email_address customer_email_address\n"
      + "        ,d_year dyear\n"
      + "        ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total\n"
      + "        ,'w' sale_type\n"
      + "  from customer\n"
      + "      ,web_sales\n"
      + "      ,date_dim\n"
      + "  where c_customer_sk = ws_bill_customer_sk\n"
      + "    and ws_sold_date_sk = d_date_sk\n"
      + "  group by c_customer_id\n"
      + "          ,c_first_name\n"
      + "          ,c_last_name\n"
      + "          ,c_preferred_cust_flag\n"
      + "          ,c_birth_country\n"
      + "          ,c_login\n"
      + "          ,c_email_address\n"
      + "          ,d_year\n"
      + "          )\n"
      + "   select\n"
      + "                   t_s_secyear.customer_id\n"
      + "                  ,t_s_secyear.customer_first_name\n"
      + "                  ,t_s_secyear.customer_last_name\n"
      + "                  ,t_s_secyear.customer_birth_country\n"
      + "  from year_total t_s_firstyear\n"
      + "      ,year_total t_s_secyear\n"
      + "      ,year_total t_c_firstyear\n"
      + "      ,year_total t_c_secyear\n"
      + "      ,year_total t_w_firstyear\n"
      + "      ,year_total t_w_secyear\n"
      + "  where t_s_secyear.customer_id = t_s_firstyear.customer_id\n"
      + "    and t_s_firstyear.customer_id = t_c_secyear.customer_id\n"
      + "    and t_s_firstyear.customer_id = t_c_firstyear.customer_id\n"
      + "    and t_s_firstyear.customer_id = t_w_firstyear.customer_id\n"
      + "    and t_s_firstyear.customer_id = t_w_secyear.customer_id\n"
      + "    and t_s_firstyear.sale_type = 's'\n"
      + "    and t_c_firstyear.sale_type = 'c'\n"
      + "    and t_w_firstyear.sale_type = 'w'\n"
      + "    and t_s_secyear.sale_type = 's'\n"
      + "    and t_c_secyear.sale_type = 'c'\n"
      + "    and t_w_secyear.sale_type = 'w'\n"
      + "    and t_s_firstyear.dyear =  1999\n"
      + "    and t_s_secyear.dyear = 1999+1\n"
      + "    and t_c_firstyear.dyear =  1999\n"
      + "    and t_c_secyear.dyear =  1999+1\n"
      + "    and t_w_firstyear.dyear = 1999\n"
      + "    and t_w_secyear.dyear = 1999+1\n"
      + "    and t_s_firstyear.year_total > 0\n"
      + "    and t_c_firstyear.year_total > 0\n"
      + "    and t_w_firstyear.year_total > 0\n"
      + "    and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end\n"
      + "            > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end\n"
      + "    and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end\n"
      + "            > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end\n"
      + "  order by t_s_secyear.customer_id\n"
      + "          ,t_s_secyear.customer_first_name\n"
      + "          ,t_s_secyear.customer_last_name\n"
      + "          ,t_s_secyear.customer_birth_country\n"
      + " limit 100";

  public static final String QUERY7 =
    "select  i_item_id, \n"
      + "        avg(ss_quantity) agg1,\n"
      + "        avg(ss_list_price) agg2,\n"
      + "        avg(ss_coupon_amt) agg3,\n"
      + "        avg(ss_sales_price) agg4 \n"
      + " from store_sales, customer_demographics, date_dim, item, promotion\n"
      + " where ss_sold_date_sk = d_date_sk and\n"
      + "       ss_item_sk = i_item_sk and\n"
      + "       ss_cdemo_sk = cd_demo_sk and\n"
      + "       ss_promo_sk = p_promo_sk and\n"
      + "       cd_gender = 'F' and \n"
      + "       cd_marital_status = 'W' and\n"
      + "       cd_education_status = 'College' and\n"
      + "       (p_channel_email = 'N' or p_channel_event = 'N') and\n"
      + "       d_year = 2001 \n"
      + " group by i_item_id\n"
      + " order by i_item_id\n"
      + " limit 100";

  public static final String QUERY11 =
    "with year_total as (\n"
      + " select c_customer_id customer_id\n"
      + "       ,c_first_name customer_first_name\n"
      + "       ,c_last_name customer_last_name\n"
      + "       ,c_preferred_cust_flag customer_preferred_cust_flag\n"
      + "       ,c_birth_country customer_birth_country\n"
      + "       ,c_login customer_login\n"
      + "       ,c_email_address customer_email_address\n"
      + "       ,d_year dyear\n"
      + "       ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total\n"
      + "       ,'s' sale_type\n"
      + " from customer\n"
      + "     ,store_sales\n"
      + "     ,date_dim\n"
      + " where c_customer_sk = ss_customer_sk\n"
      + "   and ss_sold_date_sk = d_date_sk\n"
      + " group by c_customer_id\n"
      + "         ,c_first_name\n"
      + "         ,c_last_name\n"
      + "         ,c_preferred_cust_flag \n"
      + "         ,c_birth_country\n"
      + "         ,c_login\n"
      + "         ,c_email_address\n"
      + "         ,d_year \n"
      + " union all\n"
      + " select c_customer_id customer_id\n"
      + "       ,c_first_name customer_first_name\n"
      + "       ,c_last_name customer_last_name\n"
      + "       ,c_preferred_cust_flag customer_preferred_cust_flag\n"
      + "       ,c_birth_country customer_birth_country\n"
      + "       ,c_login customer_login\n"
      + "       ,c_email_address customer_email_address\n"
      + "       ,d_year dyear\n"
      + "       ,sum(ws_ext_list_price-ws_ext_discount_amt) year_total\n"
      + "       ,'w' sale_type\n"
      + " from customer\n"
      + "     ,web_sales\n"
      + "     ,date_dim\n"
      + " where c_customer_sk = ws_bill_customer_sk\n"
      + "   and ws_sold_date_sk = d_date_sk\n"
      + " group by c_customer_id\n"
      + "         ,c_first_name\n"
      + "         ,c_last_name\n"
      + "         ,c_preferred_cust_flag \n"
      + "         ,c_birth_country\n"
      + "         ,c_login\n"
      + "         ,c_email_address\n"
      + "         ,d_year\n"
      + "         )\n"
      + "  select  \n"
      + "                  t_s_secyear.customer_id\n"
      + "                 ,t_s_secyear.customer_first_name\n"
      + "                 ,t_s_secyear.customer_last_name\n"
      + "                 ,t_s_secyear.customer_email_address\n"
      + " from year_total t_s_firstyear\n"
      + "     ,year_total t_s_secyear\n"
      + "     ,year_total t_w_firstyear\n"
      + "     ,year_total t_w_secyear\n"
      + " where t_s_secyear.customer_id = t_s_firstyear.customer_id\n"
      + "         and t_s_firstyear.customer_id = t_w_secyear.customer_id\n"
      + "         and t_s_firstyear.customer_id = t_w_firstyear.customer_id\n"
      + "         and t_s_firstyear.sale_type = 's'\n"
      + "         and t_w_firstyear.sale_type = 'w'\n"
      + "         and t_s_secyear.sale_type = 's'\n"
      + "         and t_w_secyear.sale_type = 'w'\n"
      + "         and t_s_firstyear.dyear = 1998\n"
      + "         and t_s_secyear.dyear = 1998+1\n"
      + "         and t_w_firstyear.dyear = 1998\n"
      + "         and t_w_secyear.dyear = 1998+1\n"
      + "         and t_s_firstyear.year_total > 0\n"
      + "         and t_w_firstyear.year_total > 0\n"
      + "         and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total "
      + "else 0.0 end\n"
      + "             > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total"
      + " else 0.0 end\n"
      + " order by t_s_secyear.customer_id\n"
      + "         ,t_s_secyear.customer_first_name\n"
      + "         ,t_s_secyear.customer_last_name\n"
      + "         ,t_s_secyear.customer_email_address\n"
      + "limit 100";

  public static final String QUERY12 =
    " select  i_item_id\n"
      + "       ,i_item_desc\n"
      + "       ,i_category\n"
      + "       ,i_class\n"
      + "       ,i_current_price\n"
      + "       ,sum(ws_ext_sales_price) as itemrevenue\n"
      + "       ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over\n"
      + "           (partition by i_class) as revenueratio\n"
      + " from\n"
      + "     web_sales\n"
      + "         ,item\n"
      + "         ,date_dim\n"
      + " where\n"
      + "     ws_item_sk = i_item_sk\n"
      + "     and i_category in ('Electronics', 'Books', 'Women')\n"
      + "     and ws_sold_date_sk = d_date_sk\n"
      + "     and d_date between cast('1998-01-06' as date)\n"
      + "                 and (cast('1998-01-06' as date) + 30 days)\n"
      + " group by\n"
      + "     i_item_id\n"
      + "         ,i_item_desc\n"
      + "         ,i_category\n"
      + "         ,i_class\n"
      + "         ,i_current_price\n"
      + " order by\n"
      + "     i_category\n"
      + "         ,i_class\n"
      + "         ,i_item_id\n"
      + "         ,i_item_desc\n"
      + "         ,revenueratio\n"
      + " limit 100";

  public static final String QUERY16 = // call_center, customer_address
    "select\n"
      + "    count(distinct cs_order_number) as \"order count\"\n"
      + "   ,sum(cs_ext_ship_cost) as \"total shipping cost\"\n"
      + "   ,sum(cs_net_profit) as \"total net profit\"\n"
      + " from\n"
      + "    catalog_sales cs1\n"
      + "   ,date_dim\n"
      + "   ,customer_address\n"
      + "   ,call_center\n"
      + " where\n"
      + "     d_date between '1999-4-01' and\n"
      + "            (cast('1999-4-01' as date) + 60 days)\n"
      + " and cs1.cs_ship_date_sk = d_date_sk\n"
      + " and cs1.cs_ship_addr_sk = ca_address_sk\n"
      + " and ca_state = 'IL'\n"
      + " and cs1.cs_call_center_sk = cc_call_center_sk\n"
      + " and cc_county in ('Richland County','Bronx County','Maverick County','Mesa County',\n"
      + "                   'Raleigh County'\n"
      + " )\n"
      + " and exists (select *\n"
      + "             from catalog_sales cs2\n"
      + "             where cs1.cs_order_number = cs2.cs_order_number\n"
      + "               and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)\n"
      + " and not exists(select *\n"
      + "                from catalog_returns cr1\n"
      + "                where cs1.cs_order_number = cr1.cr_order_number)\n"
      + " order by count(distinct cs_order_number)\n"
      + " limit 100";

    public static final String QUERY17 = // store_returns, store
    "select  i_item_id\n"
      + "        ,i_item_desc\n"
      + "        ,s_state\n"
      + "        ,count(ss_quantity) as store_sales_quantitycount\n"
      + "        ,avg(ss_quantity) as store_sales_quantityave\n"
      + "        ,stddev_samp(ss_quantity) as store_sales_quantitystdev\n"
      + "        ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov\n"
      + "        ,count(sr_return_quantity) as store_returns_quantitycount\n"
      + "        ,avg(sr_return_quantity) as store_returns_quantityave\n"
      + "        ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev\n"
      + "        ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov\n"
      + "        ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave\n"
      + "        ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev\n"
      + "        ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov\n"
      + "  from store_sales\n"
      + "      ,store_returns\n"
      + "      ,catalog_sales\n"
      + "      ,date_dim d1\n"
      + "      ,date_dim d2\n"
      + "      ,date_dim d3\n"
      + "      ,store\n"
      + "      ,item\n"
      + "  where d1.d_quarter_name = '2000Q1'\n"
      + "    and d1.d_date_sk = ss_sold_date_sk\n"
      + "    and i_item_sk = ss_item_sk\n"
      + "    and s_store_sk = ss_store_sk\n"
      + "    and ss_customer_sk = sr_customer_sk\n"
      + "    and ss_item_sk = sr_item_sk\n"
      + "    and ss_ticket_number = sr_ticket_number\n"
      + "    and sr_returned_date_sk = d2.d_date_sk\n"
      + "    and d2.d_quarter_name in ('2000Q1','2000Q2','2000Q3')\n"
      + "    and sr_customer_sk = cs_bill_customer_sk\n"
      + "    and sr_item_sk = cs_item_sk\n"
      + "    and cs_sold_date_sk = d3.d_date_sk\n"
      + "    and d3.d_quarter_name in ('2000Q1','2000Q2','2000Q3')\n"
      + "  group by i_item_id\n"
      + "          ,i_item_desc\n"
      + "          ,s_state\n"
      + "  order by i_item_id\n"
      + "          ,i_item_desc\n"
      + "          ,s_state\n"
      + " limit 100";

  public static final String QUERY22 =
    "select  i_product_name\n"
      + "             ,i_brand\n"
      + "             ,i_class\n"
      + "             ,i_category\n"
      + "             ,avg(inv_quantity_on_hand) qoh\n"
      + "       from inventory\n"
      + "           ,date_dim\n"
      + "           ,item\n"
      + "       where inv_date_sk=d_date_sk\n"
      + "              and inv_item_sk=i_item_sk\n"
      + "              and d_month_seq between 1200 and 1200 + 11\n"
      + "       group by rollup(i_product_name\n"
      + "                       ,i_brand\n"
      + "                       ,i_class\n"
      + "                       ,i_category)\n"
      + "order by qoh, i_product_name, i_brand, i_class, i_category\n"
      + "limit 100";

  public static final String QUERY25 = //store_returns, store
    "select\n"
      + "  i_item_id\n"
      + "  ,i_item_desc\n"
      + "  ,s_store_id\n"
      + "  ,s_store_name\n"
      + "  ,sum(ss_net_profit) as store_sales_profit\n"
      + "  ,sum(sr_net_loss) as store_returns_loss\n"
      + "  ,sum(cs_net_profit) as catalog_sales_profit\n"
      + "  from\n"
      + "  store_sales\n"
      + "  ,store_returns\n"
      + "  ,catalog_sales\n"
      + "  ,date_dim d1\n"
      + "  ,date_dim d2\n"
      + "  ,date_dim d3\n"
      + "  ,store\n"
      + "  ,item\n"
      + "  where\n"
      + "  d1.d_moy = 4\n"
      + "  and d1.d_year = 2000\n"
      + "  and d1.d_date_sk = ss_sold_date_sk\n"
      + "  and i_item_sk = ss_item_sk\n"
      + "  and s_store_sk = ss_store_sk\n"
      + "  and ss_customer_sk = sr_customer_sk\n"
      + "  and ss_item_sk = sr_item_sk\n"
      + "  and ss_ticket_number = sr_ticket_number\n"
      + "  and sr_returned_date_sk = d2.d_date_sk\n"
      + "  and d2.d_moy               between 4 and  10\n"
      + "  and d2.d_year              = 2000\n"
      + "  and sr_customer_sk = cs_bill_customer_sk\n"
      + "  and sr_item_sk = cs_item_sk\n"
      + "  and cs_sold_date_sk = d3.d_date_sk\n"
      + "  and d3.d_moy               between 4 and  10\n"
      + "  and d3.d_year              = 2000\n"
      + "  group by\n"
      + "  i_item_id\n"
      + "  ,i_item_desc\n"
      + "  ,s_store_id\n"
      + "  ,s_store_name\n"
      + "  order by\n"
      + "  i_item_id\n"
      + "  ,i_item_desc\n"
      + "  ,s_store_id\n"
      + "  ,s_store_name\n"
      + "  limit 100";

  public static final String QUERY31 = // customer_address
    "with ss as\n"
      + "  (select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as store_sales\n"
      + "  from store_sales,date_dim,customer_address\n"
      + "  where ss_sold_date_sk = d_date_sk\n"
      + "   and ss_addr_sk=ca_address_sk\n"
      + "  group by ca_county,d_qoy, d_year),\n"
      + "  ws as\n"
      + "  (select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as web_sales\n"
      + "  from web_sales,date_dim,customer_address\n"
      + "  where ws_sold_date_sk = d_date_sk\n"
      + "   and ws_bill_addr_sk=ca_address_sk\n"
      + "  group by ca_county,d_qoy, d_year)\n"
      + "  select\n"
      + "         ss1.ca_county\n"
      + "        ,ss1.d_year\n"
      + "        ,ws2.web_sales/ws1.web_sales web_q1_q2_increase\n"
      + "        ,ss2.store_sales/ss1.store_sales store_q1_q2_increase\n"
      + "        ,ws3.web_sales/ws2.web_sales web_q2_q3_increase\n"
      + "        ,ss3.store_sales/ss2.store_sales store_q2_q3_increase\n"
      + "  from\n"
      + "         ss ss1\n"
      + "        ,ss ss2\n"
      + "        ,ss ss3\n"
      + "        ,ws ws1\n"
      + "        ,ws ws2\n"
      + "        ,ws ws3\n"
      + "  where\n"
      + "     ss1.d_qoy = 1\n"
      + "     and ss1.d_year = 1999\n"
      + "     and ss1.ca_county = ss2.ca_county\n"
      + "     and ss2.d_qoy = 2\n"
      + "     and ss2.d_year = 1999\n"
      + "  and ss2.ca_county = ss3.ca_county\n"
      + "     and ss3.d_qoy = 3\n"
      + "     and ss3.d_year = 1999\n"
      + "     and ss1.ca_county = ws1.ca_county\n"
      + "     and ws1.d_qoy = 1\n"
      + "     and ws1.d_year = 1999\n"
      + "     and ws1.ca_county = ws2.ca_county\n"
      + "     and ws2.d_qoy = 2\n"
      + "     and ws2.d_year = 1999\n"
      + "     and ws1.ca_county = ws3.ca_county\n"
      + "     and ws3.d_qoy = 3\n"
      + "     and ws3.d_year =1999\n"
      + "     and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end\n"
      + "        > case when ss1.store_sales > 0 then ss2.store_sales/ss1.store_sales else null end\n"
      + "     and case when ws2.web_sales > 0 then ws3.web_sales/ws2.web_sales else null end\n"
      + "        > case when ss2.store_sales > 0 then ss3.store_sales/ss2.store_sales else null end\n"
      + "  order by ss1.d_year";

  public static final String QUERY38 =
    "select  count(*) from (\n"
      + "    select distinct c_last_name, c_first_name, d_date\n"
      + "    from store_sales, date_dim, customer\n"
      + "          where store_sales.ss_sold_date_sk = date_dim.d_date_sk\n"
      + "      and store_sales.ss_customer_sk = customer.c_customer_sk\n"
      + "      and d_month_seq between 1189 and 1189 + 11\n"
      + "  intersect\n"
      + "    select distinct c_last_name, c_first_name, d_date\n"
      + "    from catalog_sales, date_dim, customer\n"
      + "          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk\n"
      + "      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk\n"
      + "      and d_month_seq between 1189 and 1189 + 11\n"
      + "  intersect\n"
      + "    select distinct c_last_name, c_first_name, d_date\n"
      + "    from web_sales, date_dim, customer\n"
      + "          where web_sales.ws_sold_date_sk = date_dim.d_date_sk\n"
      + "      and web_sales.ws_bill_customer_sk = customer.c_customer_sk\n"
      + "      and d_month_seq between 1189 and 1189 + 11\n"
      + ") hot_cust\n"
      + "limit 100";

  public static final String QUERY42 =
    "select  dt.d_year\n"
      + "    ,item.i_category_id\n"
      + "    ,item.i_category\n"
      + "    ,sum(ss_ext_sales_price)\n"
      + " from   date_dim dt\n"
      + "    ,store_sales\n"
      + "    ,item\n"
      + " where dt.d_date_sk = store_sales.ss_sold_date_sk\n"
      + "    and store_sales.ss_item_sk = item.i_item_sk\n"
      + "    and item.i_manager_id = 1   \n"
      + "    and dt.d_moy=11\n"
      + "    and dt.d_year=1998\n"
      + " group by   dt.d_year\n"
      + "        ,item.i_category_id\n"
      + "        ,item.i_category\n"
      + " order by       sum(ss_ext_sales_price) desc,dt.d_year\n"
      + "        ,item.i_category_id\n"
      + "        ,item.i_category\n"
      + "limit 100";

  public static final String QUERY51 =
    " WITH web_v1 as (\n"
      + " select\n"
      + "   ws_item_sk item_sk, d_date,\n"
      + "   sum(sum(ws_sales_price))\n"
      + "       over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales\n"
      + " from web_sales\n"
      + "     ,date_dim\n"
      + " where ws_sold_date_sk=d_date_sk\n"
      + "   and d_month_seq between 1196 and 1196+11\n"
      + "   and ws_item_sk is not NULL\n"
      + " group by ws_item_sk, d_date),\n"
      + " store_v1 as (\n"
      + " select\n"
      + "   ss_item_sk item_sk, d_date,\n"
      + "   sum(sum(ss_sales_price))\n"
      + "       over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales\n"
      + " from store_sales\n"
      + "     ,date_dim\n"
      + " where ss_sold_date_sk=d_date_sk\n"
      + "   and d_month_seq between 1196 and 1196+11\n"
      + "   and ss_item_sk is not NULL\n"
      + " group by ss_item_sk, d_date)\n"
      + "  select  *\n"
      + " from (select item_sk\n"
      + "      ,d_date\n"
      + "      ,web_sales\n"
      + "      ,store_sales\n"
      + "      ,max(web_sales)\n"
      + "          over (partition by item_sk order by d_date rows between unbounded preceding and current row) web_cumulative\n"
      + "      ,max(store_sales)\n"
      + "          over (partition by item_sk order by d_date rows between unbounded preceding and current row) store_cumulative\n"
      + "      from (select case when web.item_sk is not null then web.item_sk else store.item_sk end item_sk\n"
      + "                  ,case when web.d_date is not null then web.d_date else store.d_date end d_date\n"
      + "                  ,web.cume_sales web_sales\n"
      + "                  ,store.cume_sales store_sales\n"
      + "            from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_sk\n"
      + "                                                           and web.d_date = store.d_date)\n"
      + "           )x )y\n"
      + " where web_cumulative > store_cumulative\n"
      + " order by item_sk\n"
      + "         ,d_date\n"
      + " limit 100";

  public static final String QUERY76 =
    "select  channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (\n"
      + "         SELECT 'store' as channel, 'ss_promo_sk' col_name, d_year, d_qoy, i_category, ss_ext_sales_price ext_sales_price\n"
      + "          FROM store_sales, item, date_dim\n"
      + "          WHERE ss_promo_sk IS NULL\n"
      + "            AND ss_sold_date_sk=d_date_sk\n"
      + "            AND ss_item_sk=i_item_sk\n"
      + "         UNION ALL\n"
      + "         SELECT 'web' as channel, 'ws_bill_addr_sk' col_name, d_year, d_qoy, i_category, ws_ext_sales_price ext_sales_price\n"
      + "          FROM web_sales, item, date_dim\n"
      + "          WHERE ws_bill_addr_sk IS NULL\n"
      + "            AND ws_sold_date_sk=d_date_sk\n"
      + "            AND ws_item_sk=i_item_sk\n"
      + "         UNION ALL\n"
      + "         SELECT 'catalog' as channel, 'cs_bill_addr_sk' col_name, d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price\n"
      + "          FROM catalog_sales, item, date_dim\n"
      + "          WHERE cs_bill_addr_sk IS NULL\n"
      + "            AND cs_sold_date_sk=d_date_sk\n"
      + "            AND cs_item_sk=i_item_sk) foo\n"
      + " GROUP BY channel, col_name, d_year, d_qoy, i_category\n"
      + " ORDER BY channel, col_name, d_year, d_qoy, i_category\n"
      + " limit 100";

  public static final String QUERY77 = // store, catalog_returns, web_page, web_returns
    "with ss as\n"
      + "  (select s_store_sk,\n"
      + "          sum(ss_ext_sales_price) as sales,\n"
      + "          sum(ss_net_profit) as profit\n"
      + "  from store_sales,\n"
      + "       date_dim,\n"
      + "       store\n"
      + "  where ss_sold_date_sk = d_date_sk\n"
      + "        and d_date between cast('2000-08-16' as date)\n"
      + "                   and (cast('2000-08-16' as date) +  30 days)\n"
      + "        and ss_store_sk = s_store_sk\n"
      + "  group by s_store_sk)\n"
      + "  ,\n"
      + "  sr as\n"
      + "  (select s_store_sk,\n"
      + "          sum(sr_return_amt) as returns,\n"
      + "          sum(sr_net_loss) as profit_loss\n"
      + "  from store_returns,\n"
      + "       date_dim,\n"
      + "       store\n"
      + "  where sr_returned_date_sk = d_date_sk\n"
      + "        and d_date between cast('2000-08-16' as date)\n"
      + "                   and (cast('2000-08-16' as date) +  30 days)\n"
      + "        and sr_store_sk = s_store_sk\n"
      + "  group by s_store_sk),\n"
      + "  cs as\n"
      + "  (select cs_call_center_sk,\n"
      + "         sum(cs_ext_sales_price) as sales,\n"
      + "         sum(cs_net_profit) as profit\n"
      + "  from catalog_sales,\n"
      + "       date_dim\n"
      + "  where cs_sold_date_sk = d_date_sk\n"
      + "        and d_date between cast('2000-08-16' as date)\n"
      + "                   and (cast('2000-08-16' as date) +  30 days)\n"
      + "  group by cs_call_center_sk\n"
      + "  ),\n"
      + "  cr as\n"
      + "  (select cr_call_center_sk,\n"
      + "          sum(cr_return_amount) as returns,\n"
      + "          sum(cr_net_loss) as profit_loss\n"
      + "  from catalog_returns,\n"
      + "       date_dim\n"
      + "  where cr_returned_date_sk = d_date_sk\n"
      + "        and d_date between cast('2000-08-16' as date)\n"
      + "                   and (cast('2000-08-16' as date) +  30 days)\n"
      + "  group by cr_call_center_sk\n"
      + "  ),\n"
      + "  ws as\n"
      + "  ( select wp_web_page_sk,\n"
      + "         sum(ws_ext_sales_price) as sales,\n"
      + "         sum(ws_net_profit) as profit\n"
      + "  from web_sales,\n"
      + "       date_dim,\n"
      + "       web_page\n"
      + "  where ws_sold_date_sk = d_date_sk\n"
      + "        and d_date between cast('2000-08-16' as date)\n"
      + "                   and (cast('2000-08-16' as date) +  30 days)\n"
      + "        and ws_web_page_sk = wp_web_page_sk\n"
      + "  group by wp_web_page_sk),\n"
      + "  wr as\n"
      + "  (select wp_web_page_sk,\n"
      + "         sum(wr_return_amt) as returns,\n"
      + "         sum(wr_net_loss) as profit_loss\n"
      + "from web_returns,\n"
      + "       date_dim,\n"
      + "       web_page\n"
      + "  where wr_returned_date_sk = d_date_sk\n"
      + "        and d_date between cast('2000-08-16' as date)\n"
      + "                   and (cast('2000-08-16' as date) +  30 days)\n"
      + "        and wr_web_page_sk = wp_web_page_sk\n"
      + "  group by wp_web_page_sk)\n"
      + "   select  channel\n"
      + "         , id\n"
      + "         , sum(sales) as sales\n"
      + "         , sum(returns) as returns\n"
      + "         , sum(profit) as profit\n"
      + "  from   \n"
      + "  (select 'store channel' as channel\n"
      + "         , ss.s_store_sk as id\n"
      + "         , sales\n"
      + "         , coalesce(returns, 0) as returns\n"
      + "         , (profit - coalesce(profit_loss,0)) as profit\n"
      + "  from   ss left join sr\n"
      + "         on  ss.s_store_sk = sr.s_store_sk\n"
      + "  union all \n"
      + "  select 'catalog channel' as channel\n"
      + "         , cs_call_center_sk as id\n"
      + "         , sales\n"
      + "         , returns\n"
      + "         , (profit - profit_loss) as profit\n"
      + "  from  cs \n"
      + "        , cr\n"
      + "  union all\n"
      + "  select 'web channel' as channel\n"
      + "         , ws.wp_web_page_sk as id\n"
      + "         , sales\n"
      + "         , coalesce(returns, 0) returns\n"
      + "         , (profit - coalesce(profit_loss,0)) as profit\n"
      + "  from   ws left join wr\n"
      + "         on  ws.wp_web_page_sk = wr.wp_web_page_sk\n"
      + "  ) x\n"
      + "  group by rollup (channel, id)\n"
      + "  order by channel\n"
      + "          ,id\n"
      + "  limit 100";

  public static final String QUERY78 =
    " with ws as\n"
      + "   (select d_year AS ws_sold_year, ws_item_sk,\n"
      + "     ws_bill_customer_sk ws_customer_sk,\n"
      + "     sum(ws_quantity) ws_qty,\n"
      + "     sum(ws_wholesale_cost) ws_wc,\n"
      + "     sum(ws_sales_price) ws_sp\n"
      + "    from web_sales\n"
      + "    left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk\n"
      + "    join date_dim on ws_sold_date_sk = d_date_sk\n"
      + "    where wr_order_number is null\n"
      + "    group by d_year, ws_item_sk, ws_bill_customer_sk\n"
      + "    ),\n"
      + " cs as\n"
      + "   (select d_year AS cs_sold_year, cs_item_sk,\n"
      + "     cs_bill_customer_sk cs_customer_sk,\n"
      + "     sum(cs_quantity) cs_qty,\n"
      + "     sum(cs_wholesale_cost) cs_wc,\n"
      + "     sum(cs_sales_price) cs_sp\n"
      + "    from catalog_sales\n"
      + "    left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk\n"
      + "    join date_dim on cs_sold_date_sk = d_date_sk\n"
      + "    where cr_order_number is null\n"
      + "    group by d_year, cs_item_sk, cs_bill_customer_sk\n"
      + "    ),\n"
      + " ss as\n"
      + "   (select d_year AS ss_sold_year, ss_item_sk,\n"
      + "     ss_customer_sk,\n"
      + "     sum(ss_quantity) ss_qty,\n"
      + "     sum(ss_wholesale_cost) ss_wc,\n"
      + "     sum(ss_sales_price) ss_sp\n"
      + "    from store_sales\n"
      + "    left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk\n"
      + "    join date_dim on ss_sold_date_sk = d_date_sk\n"
      + "    where sr_ticket_number is null\n"
      + "    group by d_year, ss_item_sk, ss_customer_sk\n"
      + "    )\n"
      + "  select\n"
      + " ss_customer_sk,\n"
      + " round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,\n"
      + " ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,\n"
      + " coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,\n"
      + " coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,\n"
      + " coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price\n"
      + " from ss\n"
      + " left join ws on (ws_sold_year=ss_sold_year and ws_item_sk=ss_item_sk and ws_customer_sk=ss_customer_sk)\n"
      + " left join cs on (cs_sold_year=ss_sold_year and cs_item_sk=ss_item_sk and cs_customer_sk=ss_customer_sk)\n"
      + " where (coalesce(ws_qty,0)>0 or coalesce(cs_qty, 0)>0) and ss_sold_year=2001\n"
      + " order by\n"
      + "   ss_customer_sk,\n"
      + "   ss_qty desc, ss_wc desc, ss_sp desc,\n"
      + "   other_chan_qty,\n"
      + "   other_chan_wholesale_cost,\n"
      + "   other_chan_sales_price,\n"
      + "   round(ss_qty/(coalesce(ws_qty+cs_qty,1)),2)\n"
      + " limit 100";

  public static final String QUERY84 =  // customer_address, income_band, store_returns
    " select  c_customer_id as customer_id\n"
      + "        , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername\n"
      + "  from customer\n"
      + "      ,customer_address\n"
      + "      ,customer_demographics\n"
      + "      ,household_demographics\n"
      + "      ,income_band\n"
      + "      ,store_returns\n"
      + "  where ca_city          =  'Bethel'\n"
      + "    and c_current_addr_sk = ca_address_sk\n"
      + "    and ib_lower_bound   >=  57955\n"
      + "    and ib_upper_bound   <=  57955 + 50000\n"
      + "    and ib_income_band_sk = hd_income_band_sk\n"
      + "    and cd_demo_sk = c_current_cdemo_sk\n"
      + "    and hd_demo_sk = c_current_hdemo_sk\n"
      + "    and sr_cdemo_sk = cd_demo_sk\n"
      + "  order by c_customer_id\n"
      + "  limit 100";

  public static final String QUERY88 =  // household_demographics, time_dim, store
    "select  *\n"
      + " from\n"
      + "  (select count(*) h8_30_to_9\n"
      + "  from store_sales, household_demographics , time_dim, store\n"
      + "  where ss_sold_time_sk = time_dim.t_time_sk\n"
      + "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n"
      + "      and ss_store_sk = s_store_sk\n"
      + "      and time_dim.t_hour = 8\n"
      + "      and time_dim.t_minute >= 30\n"
      + "      and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or\n"
      + "           (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or\n"
      + "           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))\n"
      + "      and store.s_store_name = 'ese') s1,\n"
      + "  (select count(*) h9_to_9_30\n"
      + "  from store_sales, household_demographics , time_dim, store\n"
      + "  where ss_sold_time_sk = time_dim.t_time_sk\n"
      + "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n"
      + "      and ss_store_sk = s_store_sk\n"
      + "      and time_dim.t_hour = 9\n"
      + "      and time_dim.t_minute < 30\n"
      + "      and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or\n"
      + "           (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or\n"
      + "           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))\n"
      + "      and store.s_store_name = 'ese') s2,\n"
      + "  (select count(*) h9_30_to_10\n"
      + "  from store_sales, household_demographics , time_dim, store\n"
      + "  where ss_sold_time_sk = time_dim.t_time_sk\n"
      + "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n"
      + "      and ss_store_sk = s_store_sk\n"
      + "      and time_dim.t_hour = 9\n"
      + "      and time_dim.t_minute >= 30\n"
      + "      and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or\n"
      + "           (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or\n"
      + "           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))\n"
      + "      and store.s_store_name = 'ese') s3,\n"
      + "  (select count(*) h10_to_10_30\n"
      + "  from store_sales, household_demographics , time_dim, store\n"
      + "  where ss_sold_time_sk = time_dim.t_time_sk\n"
      + "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n"
      + "      and ss_store_sk = s_store_sk\n"
      + "      and time_dim.t_hour = 10 \n"
      + "      and time_dim.t_minute < 30\n"
      + "      and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or\n"
      + "           (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or\n"
      + "           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))\n"
      + "      and store.s_store_name = 'ese') s4,\n"
      + "  (select count(*) h10_30_to_11\n"
      + "  from store_sales, household_demographics , time_dim, store\n"
      + "  where ss_sold_time_sk = time_dim.t_time_sk\n"
      + "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n"
      + "      and ss_store_sk = s_store_sk\n"
      + "      and time_dim.t_hour = 10 \n"
      + "      and time_dim.t_minute >= 30\n"
      + "      and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or\n"
      + "           (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or\n"
      + "           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))\n"
      + "      and store.s_store_name = 'ese') s5,\n"
      + "  (select count(*) h11_to_11_30\n"
      + "  from store_sales, household_demographics , time_dim, store\n"
      + "  where ss_sold_time_sk = time_dim.t_time_sk\n"
      + "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n"
      + "      and ss_store_sk = s_store_sk \n"
      + "      and time_dim.t_hour = 11\n"
      + "      and time_dim.t_minute < 30\n"
      + "      and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or\n"
      + "           (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or\n"
      + "           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))\n"
      + "      and store.s_store_name = 'ese') s6,\n"
      + "  (select count(*) h11_30_to_12\n"
      + "  from store_sales, household_demographics , time_dim, store\n"
      + "  where ss_sold_time_sk = time_dim.t_time_sk\n"
      + "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n"
      + "      and ss_store_sk = s_store_sk\n"
      + "      and time_dim.t_hour = 11\n"
      + "      and time_dim.t_minute >= 30\n"
      + "      and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or\n"
      + "           (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or\n"
      + "           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))\n"
      + "      and store.s_store_name = 'ese') s7,\n"
      + "  (select count(*) h12_to_12_30\n"
      + "  from store_sales, household_demographics , time_dim, store\n"
      + "  where ss_sold_time_sk = time_dim.t_time_sk\n"
      + "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n"
      + "      and ss_store_sk = s_store_sk\n"
      + "      and time_dim.t_hour = 12\n"
      + "      and time_dim.t_minute < 30\n"
      + "      and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or\n"
      + "           (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or\n"
      + "           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))\n"
      + "      and store.s_store_name = 'ese') s8";

  /**
   * TPC-H Schema.
   */

  public static final Schema PART_SCHEMA =
    Schema.builder()
      .addInt64Field("p_partkey")
      .addStringField("p_name")
      .addStringField("p_mfgr")
      .addStringField("p_brand")
      .addStringField("p_type")
      .addInt64Field("p_size")
      .addStringField("p_container")
      .addFloatField("p_retailprice")
      .addStringField("p_comment")
      .build();

  public static final Schema SUPPLIER_SCHEMA =
    Schema.builder()
      .addInt64Field("s_suppkey")
      .addStringField("s_name")
      .addStringField("s_address")
      .addInt64Field("s_nationkey")
      .addStringField("s_phone")
      .addFloatField("s_acctbal")
      .addStringField("s_comment")
      .build();

  public static final Schema PARTSUPP_SCHEMA =
    Schema.builder()
      .addInt64Field("ps_partkey")
      .addInt64Field("ps_suppkey")
      .addInt64Field("ps_availqty")
      .addFloatField("ps_supplycost")
      .addStringField("ps_comment")
      .build();

  public static final Schema CUSTOMER_SCHEMA =
    Schema.builder()
      .addInt64Field("c_custkey")
      .addStringField("c_name")
      .addStringField("c_address")
      .addInt64Field("c_nationkey")
      .addStringField("c_phone")
      .addFloatField("c_acctbal")
      .addStringField("c_mktsegment")
      .addStringField("c_comment")
      .build();

  public static final Schema ORDER_SCHEMA =
    Schema.builder()
      .addInt64Field("o_orderkey")
      .addInt64Field("o_custkey")
      .addStringField("o_orderstatus")
      .addFloatField("o_totalprice")
      .addStringField("o_orderdate")
      .addStringField("o_orderpriority")
      .addStringField("o_clerk")
      .addInt64Field("o_shippriority")
      .addStringField("o_comment")
      .build();


  public static final Schema LINEITEM_SCHEMA =
    Schema.builder()
      .addInt64Field("l_orderkey")
      .addInt64Field("l_partkey")
      .addInt64Field("l_suppkey")
      .addInt64Field("l_linenumber")
      .addFloatField("l_quantity")
      .addFloatField("l_extendedprice")
      .addFloatField("l_discount")
      .addFloatField("l_tax")
      .addStringField("l_returnflag")
      .addStringField("l_linestatus")
      .addStringField("l_shipdate")
      .addStringField("l_commitdate")
      .addStringField("l_receiptdate")
      .addStringField("l_shipinstruct")
      .addStringField("l_shipmode")
      .addStringField("l_comment")
      .build();

  public static final Schema NATION_SCHEMA =
    Schema.builder()
      .addInt64Field("n_nationkey")
      .addStringField("n_name")
      .addInt64Field("n_regionkey")
      .addStringField("n_comment")
      .build();

  public static final Schema REGION_SCHEMA =
    Schema.builder()
      .addInt64Field("r_regionkey")
      .addStringField("r_name")
      .addStringField("r_comment")
      .build();

}
