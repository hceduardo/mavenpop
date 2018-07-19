package com.redhat.mavenpop.DependencyComputer

import com.redhat.mavenpop.MavenPopJob
import org.apache.spark.sql.SaveMode

object DependencyComputerJob extends MavenPopJob {

  //ToDo: validata db structure given depth:
  // if conf.dependencyComputerDepth is 1, at least one conf.transitiveDepLabel relationship must exist in database
  // if conf.dependencyComputerDepth is not 1, at least one conf.directDepLabel relationship must exist in database

  def main(args: Array[String]) {

    val sessions = spark.read.parquet(conf.sessionsPath)

    val dependencyComputer: DependencyComputer = new Neo4JDependencyComputer(
      conf.neoBoltUrl, conf.neoUsername, conf.neoPassword, conf.dependencyComputerDepth)

    val sessionsWithDependencies = dependencyComputer.computeDependencies(spark, sessions)

    logger.info(s"writing sessions with dependencies to ${conf.sessionsWithDepsPath}")
    sessionsWithDependencies.write.mode(SaveMode.Overwrite).parquet(conf.sessionsWithDepsPath)

    spark.stop()
  }

}
