package com.redhat.mavenpop.Sessioniser

import org.apache.spark.sql.{ SaveMode, SparkSession }

object SessioniserJob {

  private val GavLogPath = "out/gavlog-part0000.parquet"
  private val SessionsPath = "out/sessions.parquet"

  private val MaxIdleMillis = 1 * 60 * 1000 // 1 minute in milliseconds

  def main(args: Array[String]) {

    val sparkMaster = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder.appName("Sessioniser")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    val gavLogs = spark.read.parquet(GavLogPath)
    val sessions = Sessioniser.createSessions(spark, gavLogs, MaxIdleMillis)
    sessions.write.mode(SaveMode.Overwrite).parquet(SessionsPath)

    spark.stop()
  }

}
