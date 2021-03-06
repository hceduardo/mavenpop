package com.redhat.mavenpop.DependencyComputer

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.redhat.mavenpop.{ MavenPopConfig, MavenPopJob }
import org.apache.spark.sql.{ DataFrame, SaveMode, SparkSession }
import org.apache.spark.sql.functions._

object DependencyComputerProfilerJob extends MavenPopJob {

  def main(args: Array[String]) {
    import spark.implicits._

    val (samplePrefix, resultPrefix, minSess, maxSess, stepSess, sampleCount) =
      (conf.profilerSamplePrefix, conf.sessionsBenchmarksPrefix,
        conf.profilerSessionSizeStart, conf.profilerSessionSizeEnd, conf.profilerSessionSizeStep,
        conf.profilerSamplesPerSize)
    val sampleSessionsPath = s"$samplePrefix-$minSess-$maxSess-$stepSess-$sampleCount.parquet"
    val outPrefix = s"$resultPrefix-$minSess-$maxSess-$stepSess-$sampleCount"
    val sessionCountPath = conf.sessionCountPath

    lazy val sessionsWithSize = spark.read.parquet(conf.sessionsPath).withColumn("gavsSize", size($"gavs"))

    //    val sessionsCount: DataFrame = getOrCreateSessionsCount(spark, sessionCountPath, sessionsWithSize)
    //
    //    sessionsCount.cache()

    val depths = conf.profilerDepthStart to conf.profilerDepthEnd by conf.profilerDepthStep
    val (neoUrl, neoUser, neoPass) = (conf.neoBoltUrl, conf.neoUsername, conf.neoPassword)

    val sampleSessions = getOrCreateSampleSessions(spark, conf, sampleSessionsPath, sessionsWithSize)
    sampleSessions.persist()

    for (depth <- depths) {

      val outPath = s"$outPrefix-$depth.parquet"
      val profiler = new Neo4JDependencyComputer(neoUrl, neoUser, neoPass, depth, true)
      val benchmarks = profiler.computeDependencies(spark, sampleSessions)

      logger.info(s"obtaining and writing benchmarks for depth $depth to $outPath")

      benchmarks.write.mode(SaveMode.Overwrite).parquet(outPath)

      logger.info(s"finished writing benchmarks for depth $depth to $outPath")

      sampleSessions.unpersist(true)

    }

    logger.info(s"stopping $jobName")
    spark.stop()
  }

  def getOrCreateSampleSessions(spark: SparkSession, conf: MavenPopConfig,
    path: String, sessionsWithSize: => DataFrame): DataFrame = {

    if (conf.profilerUseCacheSamples && dirExists(spark, path)) {
      logger.info(s"using disk cached sample sessions: $path")
      return spark.read.parquet(path)
    }

    logger.info("create sample sessions transformation scheduled")
    val _s = createSampleSessions(
      sessionsWithSize,
      conf.profilerSessionSizeStart, conf.profilerSessionSizeEnd, conf.profilerSessionSizeStep,
      conf.profilerSamplesPerSize)

    if (conf.profilerCacheSamples) {
      logger.info(s"caching sample sessions to disk: $path")
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

    // Initial projection to get the schema in "samples" DF
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
