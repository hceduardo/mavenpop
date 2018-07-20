package com.redhat.mavenpop.DependencyComputer

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import com.redhat.mavenpop.MavenPopJob
import org.apache.spark.sql.{DataFrame, SaveMode}

object DependencyComputerJob extends MavenPopJob {

  //ToDo: validata db structure given depth:
  // if conf.dependencyComputerDepth is 1, at least one conf.transitiveDepLabel relationship must exist in database
  // if conf.dependencyComputerDepth is not 1, at least one conf.directDepLabel relationship must exist in database

  def main(args: Array[String]) {

    val sessions = getSessions()

    val dependencyComputer: DependencyComputer = new Neo4JDependencyComputer(
      conf.neoBoltUrl, conf.neoUsername, conf.neoPassword, conf.dependencyComputerDepth)

    val enhancedSessions = dependencyComputer.computeDependencies(spark, sessions)

    logger.info(s"writing sessions with dependencies to ${conf.enhancedSessionsPath}")
    enhancedSessions.write.mode(SaveMode.Overwrite).parquet(conf.enhancedSessionsPath)

    spark.stop()
  }

  private def getSessions(): DataFrame = {
    spark.read.parquet(conf.sessionsPath).
      filter(s"startTime >= ${conf.startTime}").
      filter(s"startTime < ${conf.endTime}")
  }

}
