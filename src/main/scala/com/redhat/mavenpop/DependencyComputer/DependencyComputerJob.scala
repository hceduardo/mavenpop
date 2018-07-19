package com.redhat.mavenpop.DependencyComputer

import com.redhat.mavenpop.MavenPopJob
import org.apache.spark.sql.SaveMode

object DependencyComputerJob extends MavenPopJob {

  def main(args: Array[String]) {

    val sessions = spark.read.parquet(conf.sessionsPath)

    val dependencyComputer: DependencyComputer = new Neo4JDependencyComputer(
      conf.neoBoltUrl, conf.neoUsername, conf.neoPassword, conf.dependencyComputerDepth)

    val sessionsWithDependencies = dependencyComputer.computeDependencies(spark, sessions)

    sessionsWithDependencies.write.mode(SaveMode.Overwrite).parquet(conf.sessionsWithDepsPath)

    spark.stop()
  }

}
