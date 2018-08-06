package com.redhat.mavenpop

import java.time._
import java.time.format.DateTimeFormatter

import com.typesafe.config.{ Config, ConfigFactory }
import com.redhat.mavenpop.MavenPopConfig.getEpochMillis

object MavenPopConfig {
  val DATE_PATTERN = "yyyy-MM-dd'T'HH-mmX"

  def getEpochMillis(zonedDateTime: String): Long = {
    val zonedDT = ZonedDateTime.parse(
      zonedDateTime,
      DateTimeFormatter.ofPattern(DATE_PATTERN))

    zonedDT.toEpochSecond * 1000L
  }
}

/***
  *
  * @param configFile file in src/main/resources to read default config from
  */
class MavenPopConfig() {

  // Load resources/reference.conf by default
  // Allows override with -Dconfig.file=path/to/config-file
  private val config: Config = ConfigFactory.load()

  // Validate config under mavenpop path
  config.checkValid(config, "mavenpop")

  val sparkMasterUrl = config.getString("mavenpop.spark.masterUrl")
  val sparkEnableEventLog = config.getBoolean("mavenpop.spark.enableEventLog")

  val startTimeString = config.getString("mavenpop.date.start")
  val endTimeString = config.getString("mavenpop.date.end")

/*** in epoch millis ***/
  val startTime = getEpochMillis(startTimeString)

/*** in epoch millis ***/
  val endTime = getEpochMillis(endTimeString)

  val repologsPath = config.getString("mavenpop.path.repologs")
  val dependenciesPath = config.getString("mavenpop.path.dependencies")
  val gavLogsPath = config.getString("mavenpop.path.gavlogs")
  val sessionsPath = config.getString("mavenpop.path.sessions")
  val enhancedSessionsPath = config.getString("mavenpop.path.enhancedSessions")
  val sessionCountPath = config.getString("mavenpop.path.sessionCount")
  val profilerSamplePrefix = config.getString("mavenpop.path.sampleSessionsPrefix")
  val sessionsBenchmarksPrefix = config.getString("mavenpop.path.sessionsBenchmarksPrefix")

  val reportDir = config.getString("mavenpop.path.reportDir")

  val sessionMaxIdleMillis = config.getLong("mavenpop.sessioniser.maxIdleMillis")
  val neoBoltUrl = config.getString("mavenpop.dependencyComputer.neo4j.boltUrl")
  val neoUsername = config.getString("mavenpop.dependencyComputer.neo4j.username")
  val neoPassword = config.getString("mavenpop.dependencyComputer.neo4j.password")

  val profilerSessionSizeEnd = config.getInt("mavenpop.dependencyComputer.profiler.sessionSize.end")
  val profilerSessionSizeStart = config.getInt("mavenpop.dependencyComputer.profiler.sessionSize.start")
  val profilerSessionSizeStep = config.getInt("mavenpop.dependencyComputer.profiler.sessionSize.step")
  val profilerSamplesPerSize = config.getInt("mavenpop.dependencyComputer.profiler.samplesPerSize")
  val profilerDepthStart = config.getInt("mavenpop.dependencyComputer.profiler.depth.start")
  val profilerDepthEnd = config.getInt("mavenpop.dependencyComputer.profiler.depth.end")
  val profilerDepthStep = config.getInt("mavenpop.dependencyComputer.profiler.depth.step")
  val profilerUseCacheSamples = config.getBoolean("mavenpop.dependencyComputer.profiler.useCacheSamples")
  val profilerCacheSamples = config.getBoolean("mavenpop.dependencyComputer.profiler.cacheSamples")

  val parserWriteTransitive = config.getBoolean("mavenpop.parser.writeTransitive")
  val dependencyComputerDepth = config.getInt("mavenpop.dependencyComputer.depth")
  val gavLabel = config.getString("mavenpop.parser.label.gav")
  val directDepLabel = config.getString("mavenpop.parser.label.directDependency")
  val transitiveDepLabel = config.getString("mavenpop.parser.label.transitiveDependency")

  override def toString(): String = {
    val s: StringBuilder = new StringBuilder()

    s.append("[MavenPopConfig] = {\n")

    getClass.getDeclaredFields.foreach { f =>
      val name = f.getName
      if (name != "config") {
        f.setAccessible(true)
        s.append(s"  ${f.getName} = ${f.get(this)}\n")
      }
    }

    s.append("}").toString()
  }
}
