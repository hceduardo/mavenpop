package com.redhat.mavenpop

import com.typesafe.config.{ Config, ConfigFactory }

/***
  *
  * @param configFile file in src/main/resources to read default config from
  */
class MavenPopConfig(configFile: String) {

  val config: Config = ConfigFactory.load(configFile)

  // Validate config under mavenpop path
  config.checkValid(config, "mavenpop")

  val repologsPath = config.getString("mavenpop.path.repologs")
  val dependenciesPath = config.getString("mavenpop.path.dependencies")
  val gavLogsPath = config.getString("mavenpop.path.gavlogs")
  val sessionsPath = config.getString("mavenpop.path.sessions")
  val sessionsWithDepsPath = config.getString("mavenpop.path.sessionsWithDeps")

  val sessionMaxIdleMillis = config.getLong("mavenpop.sessions.maxIdleMillis")
  val neoBoltUrl = config.getString("mavenpop.neo4j.boltUrl")
  val neoUsername = config.getString("mavenpop.neo4j.username")
  val neoPassword = config.getString("mavenpop.neo4j.password")
}
