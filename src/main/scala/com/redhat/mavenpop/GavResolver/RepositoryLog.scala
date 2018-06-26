package com.redhat.mavenpop.GavResolver

case class RepositoryLog(
  clientId: Integer,
  timestamp: Long,
  agent: String,
  path: String) {}

object RepositoryLog {
  val PATTERN = """^(\d+) (\d+) (\S+) (\S+)""".r

  def parseLogLine(line: String): Option[RepositoryLog] = {
    line match {
      case PATTERN(clientId, timestamp, agent, path) => Some(RepositoryLog(
        clientId.toInt, timestamp.toLong, agent, path))

      case _ => None
    }
  }
}
