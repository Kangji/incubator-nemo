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

import org.apache.beam.sdk.schemas.Schema;


/**
 * TPC-H Schema.
 */
public final class TPCHQueries {

  /**
   * Private constructor.
   */
  private TPCHQueries() {
  }

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



  public static final String TPCH_QUERY1 = "select\n"
    + "    l_returnflag,\n"
    + "    l_linestatus,\n"
    + "  sum(l_quantity) as sum_qty,\n"
    + "  sum(l_extendedprice) as sum_base_price,\n"
    + "  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n"
    + "  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n"
    + "  avg(l_quantity) as avg_qty,\n"
    + "  avg(l_extendedprice) as avg_price,\n"
    + "  avg(l_discount) as avg_disc,\n"
    + "  count(*) as count_order\n"
    + "  from\n"
    + "    lineitem\n"
    + "  where\n"
    + "  l_shipdate <= date '1998-12-01' - interval '60' day (3)\n"
    + "  group by\n"
    + "  l_returnflag,\n"
    + "  l_linestatus\n"
    + "  order by\n"
    + "  l_returnflag,\n"
    + "  l_linestatus limit 10";


  public static final String TPCH_QUERY2 = "select\n"
    + "    s_acctbal,\n"
    + "    s_name,\n"
    + "    n_name,\n"
    + "    p_partkey,\n"
    + "    p_mfgr,\n"
    + "    s_address,\n"
    + "    s_phone,\n"
    + "    s_comment\n"
    + "  from\n"
    + "    part,\n"
    + "    supplier,\n"
    + "    partsupp,\n"
    + "    nation,\n"
    + "    region\n"
    + "  where\n"
    + "    p_partkey = ps_partkey\n"
    + "  and s_suppkey = ps_suppkey\n"
    + "  and p_size = 23\n"
    + "  and p_type like '%BRASS'\n"
    + "  and s_nationkey = n_nationkey\n"
    + "  and n_regionkey = r_regionkey\n"
    + "  and r_name = 'AMERICA'\n"
    + "  and ps_supplycost = (\n"
    + "    select\n"
    + "  min(ps_supplycost)\n"
    + "  from\n"
    + "    partsupp,\n"
    + "    supplier,\n"
    + "    nation,\n"
    + "    region\n"
    + "  where\n"
    + "    p_partkey = ps_partkey\n"
    + "  and s_suppkey = ps_suppkey\n"
    + "  and s_nationkey = n_nationkey\n"
    + "  and n_regionkey = r_regionkey\n"
    + "  and r_name = 'AMERICA'\n"
    + "\t)\n"
    + "  order by\n"
    + "  s_acctbal desc,\n"
    + "    n_name,\n"
    + "    s_name,\n"
    + "    p_partkey limit 10";


  public static final String TPCH_QUERY3 = "select\n"
    + "    l_orderkey,\n"
    + "  sum(l_extendedprice * (1 - l_discount)) as revenue,\n"
    + "    o_orderdate,\n"
    + "    o_shippriority\n"
    + "  from\n"
    + "    customer,\n"
    + "    orders,\n"
    + "    lineitem\n"
    + "  where\n"
    + "    c_mktsegment = 'FURNITURE'\n"
    + "  and c_custkey = o_custkey\n"
    + "  and l_orderkey = o_orderkey\n"
    + "  and o_orderdate < date '1995-03-08'\n"
    + "  and l_shipdate > date '1995-03-08'\n"
    + "  group by\n"
    + "  l_orderkey,\n"
    + "  o_orderdate,\n"
    + "  o_shippriority\n"
    + "  order by\n"
    + "  revenue desc,\n"
    + "    o_orderdate limit 10";


  public static final String TPCH_QUERY4 = "select\n"
    + "    o_orderpriority,\n"
    + "  count(*) as order_count\n"
    + "  from\n"
    + "    orders\n"
    + "  where\n"
    + "  o_orderdate >= date '1994-03-01'\n"
    + "  and o_orderdate < date '1994-03-01' + interval '3' month\n"
    + "  and exists (\n"
    + "    select\n"
    + "\t\t\t*\n"
    + "        from\n"
    + "        lineitem\n"
    + "        where\n"
    + "        l_orderkey = o_orderkey\n"
    + "        and l_commitdate < l_receiptdate\n"
    + "  )\n"
    + "  group by\n"
    + "  o_orderpriority\n"
    + "  order by\n"
    + "  o_orderpriority limit 10";


  public static final String TPCH_QUERY5 = "select\n"
    + "    n_name,\n"
    + "  sum(l_extendedprice * (1 - l_discount)) as revenue\n"
    + "  from\n"
    + "    customer,\n"
    + "    orders,\n"
    + "    lineitem,\n"
    + "    supplier,\n"
    + "    nation,\n"
    + "    region\n"
    + "  where\n"
    + "    c_custkey = o_custkey\n"
    + "  and l_orderkey = o_orderkey\n"
    + "  and l_suppkey = s_suppkey\n"
    + "  and c_nationkey = s_nationkey\n"
    + "  and s_nationkey = n_nationkey\n"
    + "  and n_regionkey = r_regionkey\n"
    + "  and r_name = 'MIDDLE EAST'\n"
    + "  and o_orderdate >= date '1996-01-01'\n"
    + "  and o_orderdate < date '1996-01-01' + interval '1' year\n"
    + "  group by\n"
    + "  n_name\n"
    + "  order by\n"
    + "  revenue desc  limit 10";


  public static final String TPCH_QUERY6 = "select\n"
    + "  sum(l_extendedprice * l_discount) as revenue\n"
    + "  from\n"
    + "    lineitem\n"
    + "  where\n"
    + "  l_shipdate >= date '1996-01-01'\n"
    + "  and l_shipdate < date '1996-01-01' + interval '1' year\n"
    + "  and l_discount between 0.08 - 0.01 and 0.08 + 0.01\n"
    + "  and l_quantity < 25 limit 10";


  public static final String TPCH_QUERY7 = "select\n"
    + "    supp_nation,\n"
    + "    cust_nation,\n"
    + "    l_year,\n"
    + "  sum(volume) as revenue\n"
    + "  from\n"
    + "    (\n"
    + "      select\n"
    + "        n1.n_name as supp_nation,\n"
    + "      n2.n_name as cust_nation,\n"
    + "      extract(year from l_shipdate) as l_year,\n"
    + "  l_extendedprice * (1 - l_discount) as volume\n"
    + "  from\n"
    + "    supplier,\n"
    + "    lineitem,\n"
    + "    orders,\n"
    + "    customer,\n"
    + "  nation n1,\n"
    + "  nation n2\n"
    + "  where\n"
    + "    s_suppkey = l_suppkey\n"
    + "  and o_orderkey = l_orderkey\n"
    + "  and c_custkey = o_custkey\n"
    + "  and s_nationkey = n1.n_nationkey\n"
    + "  and c_nationkey = n2.n_nationkey\n"
    + "  and (\n"
    + "\t\t\t\t(n1.n_name = 'ROMANIA' and n2.n_name = 'SAUDI ARABIA')\n"
    + "  or (n1.n_name = 'SAUDI ARABIA' and n2.n_name = 'ROMANIA')\n"
    + "\t\t\t)\n"
    + "  and l_shipdate between date '1995-01-01' and date '1996-12-31'\n"
    + "    ) as shipping\n"
    + "  group by\n"
    + "  supp_nation,\n"
    + "  cust_nation,\n"
    + "  l_year\n"
    + "  order by\n"
    + "  supp_nation,\n"
    + "  cust_nation,\n"
    + "  l_year limit 10";


  public static final String TPCH_QUERY8 = "select\n"
    + "    o_year,\n"
    + "  sum(case\n"
    + "      when nation = 'SAUDI ARABIA' then volume\n"
    + "        else 0\n"
    + "        end) / sum(volume) as mkt_share\n"
    + "  from\n"
    + "    (\n"
    + "      select\n"
    + "        extract(year from o_orderdate) as o_year,\n"
    + "  l_extendedprice * (1 - l_discount) as volume,\n"
    + "  n2.n_name as nation\n"
    + "    from\n"
    + "  part,\n"
    + "  supplier,\n"
    + "  lineitem,\n"
    + "  orders,\n"
    + "  customer,\n"
    + "  nation n1,\n"
    + "  nation n2,\n"
    + "    region\n"
    + "  where\n"
    + "    p_partkey = l_partkey\n"
    + "  and s_suppkey = l_suppkey\n"
    + "  and l_orderkey = o_orderkey\n"
    + "  and o_custkey = c_custkey\n"
    + "  and c_nationkey = n1.n_nationkey\n"
    + "  and n1.n_regionkey = r_regionkey\n"
    + "  and r_name = 'MIDDLE EAST'\n"
    + "  and s_nationkey = n2.n_nationkey\n"
    + "  and o_orderdate between date '1995-01-01' and date '1996-12-31'\n"
    + "  and p_type = 'ECONOMY BURNISHED TIN'\n"
    + "\t) as all_nations\n"
    + "  group by\n"
    + "  o_year\n"
    + "  order by\n"
    + "  o_year limit 10";


  public static final String TPCH_QUERY9 = "select\n"
    + "    nation,\n"
    + "    o_year,\n"
    + "  sum(amount) as sum_profit\n"
    + "  from\n"
    + "    (\n"
    + "      select\n"
    + "        n_name as nation,\n"
    + "      extract(year from o_orderdate) as o_year,\n"
    + "  l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n"
    + "    from\n"
    + "  part,\n"
    + "  supplier,\n"
    + "  lineitem,\n"
    + "  partsupp,\n"
    + "  orders,\n"
    + "  nation\n"
    + "    where\n"
    + "  s_suppkey = l_suppkey\n"
    + "  and ps_suppkey = l_suppkey\n"
    + "  and ps_partkey = l_partkey\n"
    + "  and p_partkey = l_partkey\n"
    + "  and o_orderkey = l_orderkey\n"
    + "  and s_nationkey = n_nationkey\n"
    + "  and p_name like '%pink%'\n"
    + "    ) as profit\n"
    + "  group by\n"
    + "  nation,\n"
    + "  o_year\n"
    + "  order by\n"
    + "  nation,\n"
    + "  o_year desc limit 10";


  public static final String TPCH_QUERY10 = "select\n"
    + "    c_custkey,\n"
    + "    c_name,\n"
    + "  sum(l_extendedprice * (1 - l_discount)) as revenue,\n"
    + "    c_acctbal,\n"
    + "    n_name,\n"
    + "    c_address,\n"
    + "    c_phone,\n"
    + "    c_comment\n"
    + "  from\n"
    + "    customer,\n"
    + "    orders,\n"
    + "    lineitem,\n"
    + "    nation\n"
    + "  where\n"
    + "    c_custkey = o_custkey\n"
    + "  and l_orderkey = o_orderkey\n"
    + "  and o_orderdate >= date '1994-01-01'\n"
    + "  and o_orderdate < date '1994-01-01' + interval '3' month\n"
    + "  and l_returnflag = 'R'\n"
    + "  and c_nationkey = n_nationkey\n"
    + "  group by\n"
    + "  c_custkey,\n"
    + "  c_name,\n"
    + "  c_acctbal,\n"
    + "  c_phone,\n"
    + "  n_name,\n"
    + "  c_address,\n"
    + "  c_comment\n"
    + "  order by\n"
    + "  revenue desc limit 10";


  public static final String TPCH_QUERY11 = "select\n"
    + "    ps_partkey,\n"
    + "  sum(ps_supplycost * ps_availqty) as value\n"
    + "  from\n"
    + "    partsupp,\n"
    + "    supplier,\n"
    + "    nation\n"
    + "  where\n"
    + "    ps_suppkey = s_suppkey\n"
    + "  and s_nationkey = n_nationkey\n"
    + "  and n_name = 'JAPAN'\n"
    + "  group by\n"
    + "  ps_partkey having\n"
    + "  sum(ps_supplycost * ps_availqty) > (\n"
    + "  select\n"
    + "  sum(ps_supplycost * ps_availqty) * 0.0001000000\n"
    + "  from\n"
    + "    partsupp,\n"
    + "    supplier,\n"
    + "    nation\n"
    + "  where\n"
    + "    ps_suppkey = s_suppkey\n"
    + "  and s_nationkey = n_nationkey\n"
    + "  and n_name = 'JAPAN'\n"
    + "\t\t)\n"
    + "  order by\n"
    + "  value desc limit 10";


  public static final String TPCH_QUERY12 = "select\n"
    + "    l_shipmode,\n"
    + "  sum(case\n"
    + "      when o_orderpriority = '1-URGENT'\n"
    + "        or o_orderpriority = '2-HIGH'\n"
    + "        then 1\n"
    + "        else 0\n"
    + "        end) as high_line_count,\n"
    + "  sum(case\n"
    + "      when o_orderpriority <> '1-URGENT'\n"
    + "        and o_orderpriority <> '2-HIGH'\n"
    + "        then 1\n"
    + "        else 0\n"
    + "        end) as low_line_count\n"
    + "  from\n"
    + "    orders,\n"
    + "    lineitem\n"
    + "  where\n"
    + "    o_orderkey = l_orderkey\n"
    + "  and l_shipmode in ('FOB', 'RAIL')\n"
    + "  and l_commitdate < l_receiptdate\n"
    + "  and l_shipdate < l_commitdate\n"
    + "  and l_receiptdate >= date '1996-01-01'\n"
    + "  and l_receiptdate < date '1996-01-01' + interval '1' year\n"
    + "  group by\n"
    + "  l_shipmode\n"
    + "  order by\n"
    + "  l_shipmode limit 10";


  public static final String TPCH_QUERY13 = "select\n"
    + "    c_count,\n"
    + "  count(*) as custdist\n"
    + "  from\n"
    + "    (\n"
    + "      select\n"
    + "        c_custkey,\n"
    + "      count(o_orderkey)\n"
    + "  from\n"
    + "  customer left outer join orders on\n"
    + "  c_custkey = o_custkey\n"
    + "  and o_comment not like '%pending%accounts%'\n"
    + "  group by\n"
    + "  c_custkey\n"
    + "\t) as c_orders (c_custkey, c_count)\n"
    + "  group by\n"
    + "  c_count\n"
    + "  order by\n"
    + "  custdist desc,\n"
    + "  c_count desc limit 10";


  public static final String TPCH_QUERY14 = "select\n"
    + "\t100.00 * sum(case\n"
    + "               when p_type like 'PROMO%'\n"
    + "                 then l_extendedprice * (1 - l_discount)\n"
    + "\t\telse 0\n"
    + "  end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\n"
    + "  from\n"
    + "    lineitem,\n"
    + "    part\n"
    + "  where\n"
    + "    l_partkey = p_partkey\n"
    + "  and l_shipdate >= date '1996-05-01'\n"
    + "  and l_shipdate < date '1996-05-01' + interval '1' month limit 10";


  public static final String TPCH_QUERY15 = "create view revenue0 (supplier_no, total_revenue) as\n"
    + "  select\n"
    + "    l_suppkey,\n"
    + "  sum(l_extendedprice * (1 - l_discount))\n"
    + "  from\n"
    + "    lineitem\n"
    + "  where\n"
    + "  l_shipdate >= date '1995-08-01'\n"
    + "  and l_shipdate < date '1995-08-01' + interval '3' month\n"
    + "  group by\n"
    + "  l_suppkey;\n"
    + "\n"
    + "\n"
    + "  select\n"
    + "    s_suppkey,\n"
    + "    s_name,\n"
    + "    s_address,\n"
    + "    s_phone,\n"
    + "    total_revenue\n"
    + "  from\n"
    + "    supplier,\n"
    + "    revenue0\n"
    + "  where\n"
    + "    s_suppkey = supplier_no\n"
    + "  and total_revenue = (\n"
    + "    select\n"
    + "  max(total_revenue)\n"
    + "  from\n"
    + "    revenue0\n"
    + "\t)\n"
    + "  order by\n"
    + "  s_suppkey;\n"
    + "\n"
    + "  drop view revenue0 limit 10";


  public static final String TPCH_QUERY16 = "select\n"
    + "    p_brand,\n"
    + "    p_type,\n"
    + "    p_size,\n"
    + "  count(distinct ps_suppkey) as supplier_cnt\n"
    + "  from\n"
    + "    partsupp,\n"
    + "    part\n"
    + "  where\n"
    + "    p_partkey = ps_partkey\n"
    + "  and p_brand <> 'Brand#14'\n"
    + "  and p_type not like 'SMALL POLISHED%'\n"
    + "  and p_size in (21, 26, 28, 27, 10, 43, 12, 46)\n"
    + "  and ps_suppkey not in (\n"
    + "    select\n"
    + "      s_suppkey\n"
    + "\t\tfrom\n"
    + "      supplier\n"
    + "      where\n"
    + "      s_comment like '%Customer%Complaints%'\n"
    + "  )\n"
    + "  group by\n"
    + "  p_brand,\n"
    + "  p_type,\n"
    + "  p_size\n"
    + "  order by\n"
    + "  supplier_cnt desc,\n"
    + "    p_brand,\n"
    + "    p_type,\n"
    + "    p_size limit 10";


  public static final String TPCH_QUERY17 = "select\n"
    + "  sum(l_extendedprice) / 7.0 as avg_yearly\n"
    + "  from\n"
    + "    lineitem,\n"
    + "    part\n"
    + "  where\n"
    + "    p_partkey = l_partkey\n"
    + "  and p_brand = 'Brand#42'\n"
    + "  and p_container = 'MED CASE'\n"
    + "  and l_quantity < (\n"
    + "  select\n"
    + "\t\t\t0.2 * avg(l_quantity)\n"
    + "  from\n"
    + "    lineitem\n"
    + "  where\n"
    + "    l_partkey = p_partkey\n"
    + "\t) limit 10";


  public static final String TPCH_QUERY18 = "select\n"
    + "    c_name,\n"
    + "    c_custkey,\n"
    + "    o_orderkey,\n"
    + "    o_orderdate,\n"
    + "    o_totalprice,\n"
    + "  sum(l_quantity)\n"
    + "  from\n"
    + "    customer,\n"
    + "    orders,\n"
    + "    lineitem\n"
    + "  where\n"
    + "  o_orderkey in (\n"
    + "    select\n"
    + "      l_orderkey\n"
    + "\t\tfrom\n"
    + "      lineitem\n"
    + "      group by\n"
    + "      l_orderkey having\n"
    + "      sum(l_quantity) > 313\n"
    + "    )\n"
    + "  and c_custkey = o_custkey\n"
    + "  and o_orderkey = l_orderkey\n"
    + "  group by\n"
    + "  c_name,\n"
    + "  c_custkey,\n"
    + "  o_orderkey,\n"
    + "  o_orderdate,\n"
    + "  o_totalprice\n"
    + "  order by\n"
    + "  o_totalprice desc,\n"
    + "    o_orderdate limit 10";


  public static final String TPCH_QUERY19 = "select\n"
    + "  sum(l_extendedprice* (1 - l_discount)) as revenue\n"
    + "  from\n"
    + "    lineitem,\n"
    + "    part\n"
    + "  where\n"
    + "    (\n"
    + "      p_partkey = l_partkey\n"
    + "      and p_brand = 'Brand#33'\n"
    + "      and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
    + "  and l_quantity >= 2 and l_quantity <= 2 + 10\n"
    + "  and p_size between 1 and 5\n"
    + "  and l_shipmode in ('AIR', 'AIR REG')\n"
    + "  and l_shipinstruct = 'DELIVER IN PERSON'\n"
    + "\t)\n"
    + "  or\n"
    + "    (\n"
    + "      p_partkey = l_partkey\n"
    + "      and p_brand = 'Brand#33'\n"
    + "      and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
    + "  and l_quantity >= 19 and l_quantity <= 19 + 10\n"
    + "  and p_size between 1 and 10\n"
    + "  and l_shipmode in ('AIR', 'AIR REG')\n"
    + "  and l_shipinstruct = 'DELIVER IN PERSON'\n"
    + "\t)\n"
    + "  or\n"
    + "    (\n"
    + "      p_partkey = l_partkey\n"
    + "      and p_brand = 'Brand#15'\n"
    + "      and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
    + "  and l_quantity >= 27 and l_quantity <= 27 + 10\n"
    + "  and p_size between 1 and 15\n"
    + "  and l_shipmode in ('AIR', 'AIR REG')\n"
    + "  and l_shipinstruct = 'DELIVER IN PERSON'\n"
    + "\t) limit 10";


  public static final String TPCH_QUERY20 = "select\n"
    + "    s_name,\n"
    + "    s_address\n"
    + "  from\n"
    + "    supplier,\n"
    + "    nation\n"
    + "  where\n"
    + "  s_suppkey in (\n"
    + "    select\n"
    + "      ps_suppkey\n"
    + "\t\tfrom\n"
    + "      partsupp\n"
    + "      where\n"
    + "      ps_partkey in (\n"
    + "      select\n"
    + "      p_partkey\n"
    + "      from\n"
    + "      part\n"
    + "      where\n"
    + "      p_name like 'linen%'\n"
    + "  )\n"
    + "  and ps_availqty > (\n"
    + "  select\n"
    + "\t\t\t\t\t0.5 * sum(l_quantity)\n"
    + "  from\n"
    + "    lineitem\n"
    + "  where\n"
    + "    l_partkey = ps_partkey\n"
    + "  and l_suppkey = ps_suppkey\n"
    + "  and l_shipdate >= date '1995-01-01'\n"
    + "  and l_shipdate < date '1995-01-01' + interval '1' year\n"
    + "\t\t\t)\n"
    + "        )\n"
    + "  and s_nationkey = n_nationkey\n"
    + "  and n_name = 'JAPAN'\n"
    + "  order by\n"
    + "  s_name limit 10";


  public static final String TPCH_QUERY21 = "select\n"
    + "    s_name,\n"
    + "  count(*) as numwait\n"
    + "  from\n"
    + "    supplier,\n"
    + "  lineitem l1,\n"
    + "    orders,\n"
    + "    nation\n"
    + "  where\n"
    + "    s_suppkey = l1.l_suppkey\n"
    + "  and o_orderkey = l1.l_orderkey\n"
    + "  and o_orderstatus = 'F'\n"
    + "  and l1.l_receiptdate > l1.l_commitdate\n"
    + "  and exists (\n"
    + "    select\n"
    + "\t\t\t*\n"
    + "        from\n"
    + "        lineitem l2\n"
    + "        where\n"
    + "        l2.l_orderkey = l1.l_orderkey\n"
    + "        and l2.l_suppkey <> l1.l_suppkey\n"
    + "  )\n"
    + "  and not exists (\n"
    + "    select\n"
    + "\t\t\t*\n"
    + "        from\n"
    + "        lineitem l3\n"
    + "        where\n"
    + "        l3.l_orderkey = l1.l_orderkey\n"
    + "        and l3.l_suppkey <> l1.l_suppkey\n"
    + "        and l3.l_receiptdate > l3.l_commitdate\n"
    + "  )\n"
    + "  and s_nationkey = n_nationkey\n"
    + "  and n_name = 'JORDAN'\n"
    + "  group by\n"
    + "  s_name\n"
    + "  order by\n"
    + "  numwait desc,\n"
    + "    s_name limit 10";


  public static final String TPCH_QUERY22 = "select\n"
    + "    cntrycode,\n"
    + "  count(*) as numcust,\n"
    + "  sum(c_acctbal) as totacctbal\n"
    + "  from\n"
    + "    (\n"
    + "      select\n"
    + "        substring(c_phone from 1 for 2) as cntrycode,\n"
    + "  c_acctbal\n"
    + "    from\n"
    + "  customer\n"
    + "    where\n"
    + "  substring(c_phone from 1 for 2) in\n"
    + "\t\t\t\t('22', '13', '31', '15', '33', '27', '23')\n"
    + "  and c_acctbal > (\n"
    + "  select\n"
    + "  avg(c_acctbal)\n"
    + "  from\n"
    + "    customer\n"
    + "  where\n"
    + "  c_acctbal > 0.00\n"
    + "  and substring(c_phone from 1 for 2) in\n"
    + "\t\t\t\t\t\t('22', '13', '31', '15', '33', '27', '23')\n"
    + "              )\n"
    + "  and not exists (\n"
    + "    select\n"
    + "\t\t\t\t\t*\n"
    + "            from\n"
    + "            orders\n"
    + "            where\n"
    + "            o_custkey = c_custkey\n"
    + "  )\n"
    + "\t) as custsale\n"
    + "  group by\n"
    + "  cntrycode\n"
    + "  order by\n"
    + "  cntrycode limit 10";

}
