package com.redhat.mavenpop.GavResolver

import com.redhat.mavenpop.MavenPopConfig
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ SaveMode, SparkSession }

object GavResolverJob {

  private val logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]) {

    //todo: validate dataframe schema inside functions

    // Load resources/reference.conf by default
    // Allows override with -Dconfig.file=path/to/config-file
    val conf: MavenPopConfig = new MavenPopConfig()

    logger.info(conf)

    val sparkMaster = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder.appName("GavResolver")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    val repositoryLogs = Parser.parseRepositoryLogs(spark, conf.repologsPath)
    repositoryLogs.cache()

    val dependencyRecords = Parser.parseDependencyRecords(spark, conf.dependenciesPath)
    dependencyRecords.cache()

    val gavLogs = GavResolver.resolveGavs(spark, repositoryLogs, dependencyRecords)

    gavLogs.write.mode(SaveMode.Overwrite).parquet(conf.gavLogsPath)

    spark.stop()
  }

}
