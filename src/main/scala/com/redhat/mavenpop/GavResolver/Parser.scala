package com.redhat.mavenpop.GavResolver

import org.apache.log4j.LogManager
import org.apache.spark.sql.{Dataset, SparkSession}

object Parser {

  @transient lazy val logger = LogManager.getLogger("mavenpop")

  def parseRepositoryLogs(spark: SparkSession, repologsPath: String): Dataset[RepositoryLog] = {

    import spark.implicits._

    val repositoryLogs = spark.read.textFile(repologsPath).
      flatMap(line => {

        RepositoryLog.parseLogLine(line) match {
          case Some(rl) => Some(rl)
          case None => {
            logger.error(s"""Could not parse line from repository logs: $line""")
            None
          }
        }
      })

    repositoryLogs

  }

  def parseDependencyRecords(spark: SparkSession, dependenciesPath: String): Dataset[DependencyRecord] = {

    import spark.implicits._

    val dependencyRecords = spark.read.textFile(dependenciesPath).
      flatMap(line => {

        DependencyRecord.parseLogLine(line) match {
          case Some(dr) => Some(dr)
          case None => {
            logger.error(s"""Could not parse line from dependency records: $line""")
            None
          }
        }
      })


    if(dependencyRecords.select("path").distinct.count != dependencyRecords.count){
      throw new RuntimeException("paths are not unique, verify input dependency file")
      //todo: show which are the duplicated paths
    }

    dependencyRecords
  }

}
