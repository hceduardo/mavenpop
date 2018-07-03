package com.redhat.mavenpop.GavResolver
//@transient lazy val log = LogManager.getRootLogger

case class DependencyRecord(
  path: String,
  gav: String,
  dependencies: Array[String]) {}

object DependencyRecord {

  val PATTERN = """^(\d+) (\S+) (\S+) (\S+)""".r

  def parseLogLine(line: String): Option[DependencyRecord] = {

    line match {

      case PATTERN(_, path, gav, dependencies) =>
        Some(DependencyRecord(
          path, gav, dependencies.split(",").distinct))

      case _ => None
    }
  }
}