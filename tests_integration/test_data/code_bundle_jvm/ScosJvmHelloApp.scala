// Source for scos-jvm-hello_2.12-1.0.0.jar. Not loaded at runtime — kept for reference and rebuild.
// To rebuild: scalac -classpath <spark-connect-client_2.12.jar> ScosJvmHelloApp.scala && jar cf scos-jvm-hello_2.12-1.0.0.jar com/
package com.snowflake.scos.test

import org.apache.spark.sql.SparkSession

object ScosJvmHelloApp {
  def main(args: Array[String]): Unit = {
    val tableName = if (args.nonEmpty) args(0) else "scos_jvm_hello_result"
    // Surface env vars applied by scos_jvm_handler.py::_apply_env_vars so the Snowfort test can
    // assert user-declared env_vars reached the JVM app runtime. "<unset>" when not provided.
    val envA = Option(System.getenv("SCOS_ENV_A")).getOrElse("<unset>")
    val envB = Option(System.getenv("SCOS_ENV_B")).getOrElse("<unset>")
    // GS injects this for a registered SCOS_CODE_BUNDLE SparkApplication. Empty/"<unset>" when
    // ENABLE_CODE_BUNDLE_IN_SCOS is off.
    val appId = Option(System.getenv("SNOWPARK_SUBMIT_SPARK_APPLICATION_ID")).getOrElse("<unset>")
    val spark = SparkSession.builder().getOrCreate()
    try {
      spark.sql(
        s"CREATE OR REPLACE TABLE $tableName (result STRING, env_a STRING, env_b STRING, app_id STRING)"
      )
      spark.sql(s"INSERT INTO $tableName VALUES ('SCOS_JVM_OK', '$envA', '$envB', '$appId')")
    } finally {
      spark.close()
    }
  }
}
