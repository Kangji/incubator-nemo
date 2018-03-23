/*
 * Copyright (C) 2018 Seoul National University
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

package edu.snu.nemo.examples.spark.sql

import com.databricks.spark.sql.perf.Query
import com.databricks.spark.sql.perf.tpcds.{TPCDS, TPCDSTables}
import org.apache.spark.sql.SparkSession
;

object SparkBenchmark {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("yarn")
      .config("spark.yarn.am.memoryOverhead", "4800mb")
      .config("spark.yarn.am.memory", "11200mb")
      .config("spark.yarn.executor.memoryOverhead", "2400mb")
      .config("spark.executor.extraClassPath", "/home/ubuntu/hadoop/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar:/home/ubuntu/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.2.jar")
      .config("spark.driver.extraClassPath", "/home/ubuntu/hadoop/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar:/home/ubuntu/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.2.jar")
      .config("spark.yarn.preserve.staging.files", "true")
      .config("spark.shuffle.compress", "false")
      .config("spark.executor.memory", "5600mb")
      .config("spark.executor.cores", "1")
      .config("spark.executor.instances", "6")
      .appName("sparkBenchmark")
      .getOrCreate()
    val sqlContext = session.sqlContext


    // SETUP
    println("BASEDIR: " + System.getProperty("user.home"))
    val baseDir = System.getProperty("user.home")
    val rootDir = baseDir + "/xvdb/tpcds/data"
    val dsdgenDir = baseDir + "/xvdb/tpcds-kit/tools" // location of dsdgen
    val databaseName = "TPCDS"
    val scaleFactor = "10" // scaleFactor defines the size of the dataset to generate (in GB).
    val format = "parquet"

    // Run:
    val tables = new TPCDSTables(sqlContext = sqlContext,
      dsdgenDir = dsdgenDir,
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType

    tables.genData(
      location = rootDir,
      format = format,
      overwrite = true, // overwrite the data that is already there
      partitionTables = true, // create the partitioned fact tables
      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
      tableFilter = "", // "" means generate all tables
      numPartitions = 100) // how many dsdgen partitions to run - number of input tasks.

    // Create the specified database
    session.sql(s"create database $databaseName")
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // For CBO only, gather statistics on all columns:
    tables.analyzeTables(databaseName, analyzeColumns = true)


    // RUN BENCHMARK
    val tpcds = new TPCDS(sqlContext = sqlContext)
    // Set:
    val resultLocation: String = baseDir + "/xvdb/tpcds/result" // place to write results
    val iterations: Int = 1 // how many iterations of queries to run.
    val queries: Seq[Query] = tpcds.tpcds2_4Queries // queries to run.
    val timeout: Int = 24 * 60 * 60 // timeout, in seconds.
    // Run:
    session.sql(s"use $databaseName")
    val experiment = tpcds.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)
    experiment.waitForFinish(timeout)



    // Get all experiments results.
    val resultTable = session.read.json(resultLocation)
    resultTable.createOrReplaceTempView("sqlPerformance")
    sqlContext.table("sqlPerformance")
  }
}
