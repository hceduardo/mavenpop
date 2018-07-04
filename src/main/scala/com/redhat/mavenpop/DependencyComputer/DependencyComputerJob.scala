package com.redhat.mavenpop.DependencyComputer

import com.redhat.mavenpop.MavenPopConfig
import org.apache.spark.sql.{ SaveMode, SparkSession }

object DependencyComputerJob {

  def main(args: Array[String]) {

    // Load resources/reference.conf by default
    // Allows override with -Dconfig.file=path/to/config-file
    val conf: MavenPopConfig = new MavenPopConfig()

    val sparkMaster = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder.appName("DependencyComputer")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    val sessions = spark.read.parquet(conf.sessionsPath)
    val sessionsAnalyser: DependencyComputer = new Neo4JDependencyComputer(
      conf.neoBoltUrl, conf.neoUsername, conf.neoPassword)
    val sessionsWithDependencies = sessionsAnalyser.computeDependencies(spark, sessions)
    sessionsWithDependencies.write.mode(SaveMode.Overwrite).parquet(conf.sessionsWithDepsPath)

    spark.stop()
  }

}
