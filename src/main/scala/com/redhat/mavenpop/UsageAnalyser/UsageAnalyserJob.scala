package com.redhat.mavenpop.UsageAnalyser

import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }

object UsageAnalyserJob {

  private val GavLogPath = "out/gavlog-part0000.parquet"
  private val SessionsPath = "out/sessions.parquet"

  private val MaxIdleMillis = 1 * 60 * 1000 // 1 minute in milliseconds

  private val Neo4jboltUrl = "bolt://localhost:7687"
  private val Neo4jUsername = "mavenpop"
  private val Neo4jPassword = "mavenpop"

  def main(args: Array[String]) {

    val sparkMaster = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder.appName("UsageAnalyser")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

//    val gavLogs = spark.read.parquet(GavLogPath)
//    val sessions = SessionBuilder.createSessions(spark, gavLogs, MaxIdleMillis)
//    sessions.write.mode(SaveMode.Overwrite).parquet(SessionsPath)

    val sessions = spark.read.parquet(SessionsPath)

    val sessionsAnalyser: SessionAnalyser = new Neo4jSessionAnalyser(Neo4jboltUrl,Neo4jUsername,Neo4jPassword)

    val  sessionsWithDependencies = sessionsAnalyser.computeDependencies(spark, sessions)

    spark.stop()
  }

}
