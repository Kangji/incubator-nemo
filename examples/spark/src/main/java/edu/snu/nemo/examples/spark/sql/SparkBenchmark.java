package edu.snu.nemo.examples.spark.sql;

import com.databricks.spark.sql.perf.tpcds.TPCDSTables;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Benchmark spark sql.
 */
public class SparkBenchmark {
  private SparkBenchmark() {
  }

  public static void main(final String[] args) {
    // SETUP
    final SparkSession session = SparkSession.builder().appName("SparkBenmark").getOrCreate();
    final SQLContext sqlContext = new SQLContext(session);
    final String dsdgenDir = "~/tpcds/tpcds-kit/tools"; // location of dsdgen
    final String scaleFactor = "10"; // scaleFactor defines the size of the dataset to generate (in GB).
    final TPCDSTables tables = new TPCDSTables(sqlContext, dsdgenDir, scaleFactor, false, false);

    final String rootDir = "~/xvdb/tpcds/data";
    final String format = "parquet";
    tables.genData(rootDir, format, true, true, true, false, "", 100);

    final String databaseName = "TPCDS";
    // Create the specified database
    session.sql("create database $databaseName");
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(rootDir, format, databaseName, true, true, "");
    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // For CBO only, gather statistics on all columns:
    tables.analyzeTables(databaseName, true, "");

    // RUN BENCHMARK
  }
}
