package com.redhat.mavenpop.DependencyComputer

import com.redhat.mavenpop.MavenPopConfig
import org.apache.spark.sql.{ DataFrame, SaveMode, SparkSession }
import org.apache.spark.sql.functions._

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

    val sessions = spark.read.parquet(conf.sessionsPath)

    val sessionsWithSize = sessions.withColumn("size", size($"gavs"))
    //    val sampleSessions = takeSampleSessions(sessionsWithSize, 10, 40)


    val sampleSessions = takeSampleSessions(
      sessionsWithSize,
      conf.profilerMinSessionSize, conf.profilerMaxSessionSize, conf.profilerSamplesPerSize)

    sampleSessions.cache()

    val dependencyProfiler: DependencyComputer =
      new Neo4JDependencyComputerProfiler(conf.neoBoltUrl, conf.neoUsername, conf.neoPassword)

    val sessionsWithComputingTime = dependencyProfiler.computeDependencies(spark, sampleSessions)

    sessionsWithComputingTime.write.mode(SaveMode.Overwrite).parquet(conf.sessionsWithTimePath)

    spark.stop()
  }
  
  private def takeSampleSessions(
    sessionsWithSize: DataFrame,
    minSessionSize: Int, maxSessionSize: Int,
    samplesPerSize: Int): DataFrame = {

    assert(minSessionSize >= 2)
    assert(maxSessionSize >= minSessionSize)

    var sessionSize = minSessionSize
    var samples = sessionsWithSize.filter(s"size == $sessionSize").limit(samplesPerSize)

    while (sessionSize < maxSessionSize) {
      sessionSize += 1
      val s = sessionsWithSize.filter(s"size == $sessionSize").limit(samplesPerSize)
      samples = samples.union(s)
    }

    samples

  }

}
