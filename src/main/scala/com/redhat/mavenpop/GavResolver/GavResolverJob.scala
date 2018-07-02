package com.redhat.mavenpop.GavResolver

import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }

object GavResolverJob {

  val RepologPath = "in/repolog-part0000.txt"
  val DependenciesPath = "in/distinct_paths_inferred_gavs_with_deps.txt"
  val GavLogPath = "out/gavlog-part0000.parquet"

  def main(args: Array[String]) {

    //todo: validate dataframe schema inside functions

    val sparkMaster = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder.appName("GavResolver")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    val repositoryLogs = Parser.parseRepositoryLogs(spark, RepologPath)
    repositoryLogs.cache()

    val dependencyRecords = Parser.parseDependencyRecords(spark, DependenciesPath)
    dependencyRecords.cache()

    val gavLogs = GavResolver.resolveGavs(spark, repositoryLogs, dependencyRecords)

    gavLogs.write.mode(SaveMode.Overwrite).parquet(GavLogPath)

    spark.stop()
  }

}
