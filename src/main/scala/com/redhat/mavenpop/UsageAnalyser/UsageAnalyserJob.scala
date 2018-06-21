package com.redhat.mavenpop.UsageAnalyser

import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }

object UsageAnalyserJob {

  val GavLogPath = "out/gavlog-part0000.parquet"
  val SessionsPath = "out/sessions.parquet"

  val maxIdleMillis = 1 * 60 * 1000 // 1 minute in milliseconds

  def main(args: Array[String]) {

    val sparkMaster = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder.appName("UsageAnalyser")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    val gavLogs = spark.read.parquet(GavLogPath)

    val sessions = SessionBuilder.createSessions(spark, gavLogs, maxIdleMillis)

    sessions.write.mode(SaveMode.Overwrite).parquet(SessionsPath)

    spark.stop()
  }

}
