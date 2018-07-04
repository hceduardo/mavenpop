package com.redhat.mavenpop

import com.typesafe.config.{ Config, ConfigFactory }

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

  val repologsPath = config.getString("mavenpop.path.repologs")
  val dependenciesPath = config.getString("mavenpop.path.dependencies")
  val gavLogsPath = config.getString("mavenpop.path.gavlogs")
  val sessionsPath = config.getString("mavenpop.path.sessions")
  val sessionsWithDepsPath = config.getString("mavenpop.path.sessionsWithDeps")

  val sessionMaxIdleMillis = config.getLong("mavenpop.sessions.maxIdleMillis")
  val neoBoltUrl = config.getString("mavenpop.neo4j.boltUrl")
  val neoUsername = config.getString("mavenpop.neo4j.username")
  val neoPassword = config.getString("mavenpop.neo4j.password")

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
