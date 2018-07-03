package com.redhat.mavenpop.DependencyComputer

import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }

object DependencyComputerJob {

  private val GavLogPath = "out/gavlog-part0000.parquet"
  private val SessionsPath = "out/sessions.parquet"
  private val SesionsWithDepPath = "out/sessions-with-dep.parquet"

  private val MaxIdleMillis = 1 * 60 * 1000 // 1 minute in milliseconds

  private val Neo4jboltUrl = "bolt://localhost:7687"
  private val Neo4jUsername = "neo4j"
  private val Neo4jPassword = "Neo03"

  def main(args: Array[String]) {

    val sparkMaster = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder.appName("DependencyComputer")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    val sessions = spark.read.parquet(SessionsPath)
    val sessionsAnalyser: DependencyComputer = new Neo4JDependencyComputer(Neo4jboltUrl, Neo4jUsername, Neo4jPassword)
    val sessionsWithDependencies = sessionsAnalyser.computeDependencies(spark, sessions)
    sessionsWithDependencies.write.mode(SaveMode.Overwrite).parquet(SesionsWithDepPath)

    spark.stop()
  }

}
