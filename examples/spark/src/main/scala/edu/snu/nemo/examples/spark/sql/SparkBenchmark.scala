package edu.snu.nemo.examples.spark.sql

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.{SQLContext, SparkSession}
;

object SparkBenchmark {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkBenchmark").getOrCreate();
    val sqlContext = new SQLContext(session);


    // SETUP
    val rootDir = "~/xvdb/tpcds/data"
    val dsdgenDir = "~/tpcds/tpcds-kit/tools" // location of dsdgen
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
    val resultLocation: String = "~/xvdb/tpcds/result" // place to write results
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
    val resultTable = spark.read.json(resultLocation)
    resultTable.createOrReplaceTempView("sqlPerformance")
    sqlContext.table("sqlPerformance")
  }
}
