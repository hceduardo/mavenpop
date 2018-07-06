package com.redhat.mavenpop.DependencyComputer

import com.redhat.mavenpop.MavenPopConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DependencyComputerProfilerJob {

  def main(args: Array[String]) {

    // Load resources/reference.conf by default
    // Allows override with -Dconfig.file=path/to/config-file
    val conf: MavenPopConfig = new MavenPopConfig()

    val sparkMaster = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder.appName("DependencyComputer")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    import spark.implicits._

    val sessionsWithSize = spark.read.parquet(conf.sessionsPath).withColumn("size", size($"gavs"))

    val sampleSessions = takeSampleSessions(sessionsWithSize, 10, 40)

    val dependencyProfiler: DependencyComputer =
      new Neo4JDependencyComputerProfiler(conf.neoBoltUrl, conf.neoUsername, conf.neoPassword)

    val sessionsWithComputingTime = dependencyProfiler.computeDependencies(spark, sessionsWithSize)

    sessionsWithComputingTime.write.mode(SaveMode.Overwrite).parquet(conf.sessionsWithTimePath)

    spark.stop()
  }

  def takeSampleSessions(sessionsWithSize: DataFrame, samplesPerSize: Int, maxSessionSize: Int): DataFrame ={

    assert(maxSessionSize > 1)

    var sessionSize = 1
    var samples = sessionsWithSize.filter(s"size == $sessionSize").limit(samplesPerSize)

    while (sessionSize <= maxSessionSize){
      sessionSize = sessionSize + 1
      val s = sessionsWithSize.filter(s"size == $sessionSize").limit(samplesPerSize)
      samples = samples.union(s)
    }

  }

}
