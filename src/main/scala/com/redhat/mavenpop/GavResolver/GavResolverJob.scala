package com.redhat.mavenpop.GavResolver

import com.redhat.mavenpop.MavenPopJob
import org.apache.spark.sql.SaveMode

object GavResolverJob extends MavenPopJob {

  def main(args: Array[String]) {

    //todo: validate dataframe schema inside functions

    logger.info(conf)

    logger.info("parsing repository logs")
    val repositoryLogs = Parser.parseRepositoryLogs(spark, conf.repologsPath)
    repositoryLogs.cache()

    logger.info("parsing dependency records")
    val dependencyRecords = Parser.parseDependencyRecords(spark, conf.dependenciesPath)
    dependencyRecords.cache()

    val gavLogs = GavResolver.resolveGavs(spark, repositoryLogs, dependencyRecords)

    logger.info("resolving gavs and writing gavlogs")
    gavLogs.write.mode(SaveMode.Overwrite).parquet(conf.gavLogsPath)

    logger.info(s"stopping $jobName")
    spark.stop()
  }

}
