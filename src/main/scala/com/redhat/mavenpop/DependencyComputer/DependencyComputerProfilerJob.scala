package com.redhat.mavenpop.DependencyComputer

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.redhat.mavenpop.MavenPopConfig
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, SaveMode, SparkSession }
import org.apache.spark.sql.functions._

object DependencyComputerProfilerJob {

  private val logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]) {

    // Load resources/reference.conf by default
    // Allows override with -Dconfig.file=path/to/config-file
    val conf: MavenPopConfig = new MavenPopConfig()

    logger.info(s"starting ${this.getClass.getSimpleName}")

    val sparkMaster = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder.appName("DependencyComputerProfiler")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    import spark.implicits._

    val (samplePrefix, resultPrefix, minSess, maxSess, sampleCount) =
      (conf.profilerSamplePrefix, conf.sessionsBenchmarksPrefix,
        conf.profilerSessionSizeStart, conf.profilerSessionSizeEnd, conf.profilerSamplesPerSize)
    val sampleSessionsPath = s"$samplePrefix-$minSess-$maxSess-$sampleCount.parquet"
    val outPrefix = s"$resultPrefix-$minSess-$maxSess-$sampleCount"
    val sessionCountPath = conf.sessionCountPath

    lazy val sessionsWithSize = spark.read.parquet(conf.sessionsPath).withColumn("gavsSize", size($"gavs"))

    //    val sessionsCount: DataFrame = getOrCreateSessionsCount(spark, sessionCountPath, sessionsWithSize)
    //
    //    sessionsCount.cache()

    val depths = conf.profilerDepthStart to conf.profilerDepthEnd by conf.profilerDepthStep
    val (neoUrl, neoUser, neoPass) = (conf.neoBoltUrl, conf.neoUsername, conf.neoPassword)

    for (depth <- depths) {

      val sampleSessions = getOrCreateSampleSessions(spark, conf, sampleSessionsPath, sessionsWithSize)

      sampleSessions.persist()

      val outPath = s"$outPrefix-$depth.parquet"
      val profiler = new Neo4JDependencyComputerProfiler(neoUrl, neoUser, neoPass, depth)
      val benchmarks = profiler.computeDependencies(spark, sampleSessions)

      logger.info(s"obtaining and writing benchmarks for depth $depth to $outPath")

      benchmarks.write.mode(SaveMode.Overwrite).parquet(outPath)

      logger.info(s"finished writing benchmarks for depth $depth to $outPath")

      sampleSessions.unpersist(true)

    }

    spark.stop()
    logger.info(s"finishing ${this.getClass.getSimpleName}")
  }

  def getOrCreateSampleSessions(spark: SparkSession, conf: MavenPopConfig,
    path: String, sessionsWithSize: => DataFrame): DataFrame = {

    if (conf.profilerUseCacheSamples && dirExists(spark, path)) {
      logger.info("using disk cached sample sessions")
      return spark.read.parquet(path)
    }

    logger.info("create sample sessions transformation scheduled")
    val _s = createSampleSessions(
      sessionsWithSize,
      conf.profilerSessionSizeStart, conf.profilerSessionSizeEnd, conf.profilerSessionSizeStep,
      conf.profilerSamplesPerSize)

    if (conf.profilerCacheSamples) {
      logger.info("caching sample sessions to disk")
      _s.write.parquet(path)
    }

    return _s

  }

  private def getOrCreateSessionsCount(spark: SparkSession, path: String, sessionsWithSize: => DataFrame): DataFrame = {
    dirExists(spark, path) match {
      case true => {
        logger.info("using disk cached sessions count")
        return spark.read.parquet(path)
      }
      case false => {
        val _s = createSessionCount(sessionsWithSize)
        logger.info("caching sessions count to disk")
        _s.write.parquet(path)
        return _s
      }
    }
  }

  def createSessionCount(sessionsWithSize: DataFrame): DataFrame = {
    return sessionsWithSize.select("gavsSize").groupBy("gavsSize").count
  }

  private def createSampleSessions(
    sessionsWithSize: DataFrame,
    minSessionSize: Int, maxSessionSize: Int, sessionSizeStep: Int,
    samplesPerSize: Int): DataFrame = {

    assert(minSessionSize >= 2)
    assert(maxSessionSize >= minSessionSize)

    var sessionSize = minSessionSize
    var samples = sessionsWithSize.filter(s"gavsSize == $sessionSize").limit(samplesPerSize)
    sessionSize += sessionSizeStep

    while (sessionSize <= maxSessionSize) {
      val s = sessionsWithSize.filter(s"gavsSize == $sessionSize").limit(samplesPerSize)
      samples = samples.union(s)
      sessionSize += sessionSizeStep
    }

    return samples

  }

  private def dirExists(spark: SparkSession, path: String): Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.exists(new Path(path))
  }

}
