package com.redhat.mavenpop.DependencyParser

import java.io.{PrintWriter, StringWriter}

import com.redhat.mavenpop.GavResolver.DependencyRecord
import org.apache.log4j.LogManager

import scala.io.Source

class NeoDataParser {

  private val NodeLabelGav = "GAV"
  private val RelLabelDep = "DEPENDS_ON"
  private var gavCount: Integer = 0
  private var depCount: Integer = 0

  private val logger = LogManager.getLogger(getClass.getName)

  def parseDependencies(inFilename: String, outGavFilename: String, outDepFilename: String): Unit = {
    var source: Source = null
    var outGav: PrintWriter = null
    var outDep: PrintWriter = null

    var exceptionCatched = false

    try {
      source = Source.fromFile(inFilename)
      outGav = new PrintWriter(outGavFilename)
      outDep = new PrintWriter(outDepFilename)

      parseLines(source, outGav, outDep)

      println("[NeoDataParser] INFO: Generated:")
      println(s"${gavCount} gavs (not unique) in ${outGavFilename}")
      println(s"${depCount} dependencies (not unique) in ${outDepFilename}")
    } catch {
      //Todo: catch different exceptions and display specific error messages
      case e: Exception => println("[NeoDataParser] ERROR:")
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println(sw.toString)
        exceptionCatched = true
    } finally {
      source.close()
      outGav.close()
      outDep.close()
      if (exceptionCatched) {
        System.exit(1)
      }
    }
  }

  private def parseLines(source: Source, outGav: PrintWriter, outDep: PrintWriter) = {
    outGav.println("id:ID,:LABEL")
    outDep.println(":START_ID,:END_ID,:TYPE")

    var lineNumber = 0

    source.getLines.foreach { line =>

      lineNumber += 1

      DependencyRecord.parseLogLine(line) match {

        case Some(depRecord) => {
          outGav.println(s"${depRecord.gav},${NodeLabelGav}")
          gavCount += 1

          depRecord.dependencies.foreach{ dep =>
            outGav.println(s"${dep},${NodeLabelGav}")
            outDep.println(s"${depRecord.gav},${dep},${RelLabelDep}")
            gavCount += 1
            depCount += 1
          }
        }

        case None => {
          logger.error(s"could not parse line ${lineNumber}: ${line}")
        }
      }
    }
  }
}
