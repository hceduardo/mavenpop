package com.redhat.mavenpop.GavResolver

import com.redhat.mavenpop.MavenPopJob
import org.apache.spark.sql.SaveMode

object GavResolverJob extends MavenPopJob {

  def main(args: Array[String]) {

    //todo: validate dataframe schema inside functions

    logger.info(conf)

    val repositoryLogs = Parser.parseRepositoryLogs(spark, conf.repologsPath)
    repositoryLogs.cache()

    val dependencyRecords = Parser.parseDependencyRecords(spark, conf.dependenciesPath)
    dependencyRecords.cache()

    val gavLogs = GavResolver.resolveGavs(spark, repositoryLogs, dependencyRecords)

    gavLogs.write.mode(SaveMode.Overwrite).parquet(conf.gavLogsPath)

    spark.stop()
  }

}
