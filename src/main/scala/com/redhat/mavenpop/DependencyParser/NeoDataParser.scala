package com.redhat.mavenpop.DependencyParser

import java.io.PrintWriter

import com.redhat.mavenpop.GavResolver.DependencyRecord
import org.apache.log4j.LogManager

import scala.io.Source

object NeoDataParser {
  val DELIMITER = "|"
  val LABEL_NODE_GAV = "GAV"
  val LABEL_REL_DEP = "DEPENDS_ON"
  val HEADER_NODE_GAV = s"id:ID$DELIMITER:LABEL"
  val HEADER_REL_DEP = s":START_ID$DELIMITER:END_ID$DELIMITER:TYPE"
}

class NeoDataParser {

  private var _gavCount: Integer = 0
  private var _depCount: Integer = 0

  def gavCount = _gavCount
  def depCount = _depCount

  private val logger = LogManager.getLogger(getClass.getName)

  def parseDependencies(source: Source, outGav: PrintWriter, outDep: PrintWriter) = {
    outGav.println(NeoDataParser.HEADER_NODE_GAV)
    outDep.println(NeoDataParser.HEADER_REL_DEP)

    // Set to manage uniqueness of gavs. Initialize the set with gavs to exclude
    val gavs = collection.mutable.Set[String]("UNKNOWN_DEPS", "NO_DEPS")

    var lineNumber = 0

    source.getLines.foreach { line =>

      lineNumber += 1

      DependencyRecord.parseLogLine(line) match {

        case Some(depRecord) => {

          // Only add dependencies of unknown gavs (handle lines with duplicate "main" gavs)

          if (gavs.add(depRecord.gav)) {

            outGav.println(s"${depRecord.gav}${NeoDataParser.DELIMITER}${NeoDataParser.LABEL_NODE_GAV}")
            _gavCount += 1

            // Dependencies returned are unique
            depRecord.dependencies.foreach { dep =>

              if (dep != "UNKNOWN_DEPS" && dep != "NO_DEPS") {
                outDep.println(s"${depRecord.gav}${NeoDataParser.DELIMITER}${dep}${NeoDataParser.DELIMITER}${NeoDataParser.LABEL_REL_DEP}")
                _depCount += 1

                // add gavs still not handled as "main" gavs
                if (gavs.add(dep)) {
                  outGav.println(s"${dep}${NeoDataParser.DELIMITER}${NeoDataParser.LABEL_NODE_GAV}")
                  _gavCount += 1
                }
              }
            }
          }
        }

        case None => {
          logger.warn(s"could not parse line ${lineNumber}: ${line}")
        }
      }
    }
  }
}
