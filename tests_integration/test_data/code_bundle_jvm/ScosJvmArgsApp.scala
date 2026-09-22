// Source for scos-jvm-args_2.12-1.0.0.jar. Not loaded at runtime — kept for reference and rebuild.
// To rebuild: scalac -classpath <spark-connect-client_2.12.jar> ScosJvmArgsApp.scala && jar cf scos-jvm-args_2.12-1.0.0.jar com/
package com.snowflake.scos.test

import org.apache.spark.sql.SparkSession

// Echoes command-line arguments into a result table so Snowfort tests can verify the exact
// tokens that EXECUTE CODE BUNDLE delivers to args[].
//   args(0)   — table name to write into (required)
//   args(1..) — tokens to echo; each becomes one row (idx INT, arg STRING)
object ScosJvmArgsApp {
  def main(args: Array[String]): Unit = {
    val table = if (args.nonEmpty) args(0) else "scos_jvm_args_result"
    val spark = SparkSession.builder().getOrCreate()
    try {
      spark.sql(s"CREATE OR REPLACE TABLE $table (idx INT, arg STRING)")
      args.drop(1).zipWithIndex.foreach { case (v, i) =>
        val escaped = v.replace("'", "''")
        spark.sql(s"INSERT INTO $table VALUES ($i, '$escaped')")
      }
    } finally {
      spark.close()
    }
  }
}
