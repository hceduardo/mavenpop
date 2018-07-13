package com.redhat.mavenpop.DependencyParser

import java.io.PrintWriter

import com.redhat.mavenpop.GavResolver.DependencyRecord

import scala.io.Source
import org.apache.log4j.LogManager

import scala.collection.mutable

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
  val dependencyMap = new DependencyMap()

  class DependencyMap(){
    private val map = mutable.Map[String, scala.collection.mutable.Set[String]]()
    private val excludedDeps = Set[String]("UNKNOWN_DEPS", "NO_DEPS")

    def isEmpty: Boolean = map.isEmpty

    def getEntries: Iterator[(String, mutable.Set[String])] = {map.iterator}

    def add(gav: String, dependencies: Set[String]): Unit ={

      val _deps = dependencies -- excludedDeps

      _deps.foreach(dep => {
        if(!map.contains(dep)){
          map(dep) = mutable.Set.empty[String]
        }
      })

      map.contains(gav) match {
        case false => map(gav) = mutable.Set(_deps.toSeq: _*)
        case true => map(gav) ++= _deps
      }
    }

  }

  private def parseDependencyMap(source: Source): Unit = {

    var lineNumber = 0

    source.getLines.foreach(line => {

      lineNumber += 1
      DependencyRecord.parseLogLine(line) match {
        case None => logger.warn(s"could not parse line ${lineNumber}: ${line}")
        case Some(dependencyRecord) => {
          dependencyMap.add(dependencyRecord.gav, dependencyRecord.dependencies.toSet)
        }
      }

    })
  }

  private def writeDependencyMap(outGav: PrintWriter, outDep: PrintWriter) = {
    assert(!dependencyMap.isEmpty)

    outGav.println(NeoDataParser.HEADER_NODE_GAV)
    outDep.println(NeoDataParser.HEADER_REL_DEP)

    dependencyMap.getEntries.foreach{ case(gav: String, dependencies: mutable.Set[String]) =>

        outGav.println(s"${gav}${NeoDataParser.DELIMITER}${NeoDataParser.LABEL_NODE_GAV}")
        _gavCount += 1

        dependencies.foreach{dep =>
          outDep.println(s"${gav}${NeoDataParser.DELIMITER}${dep}${NeoDataParser.DELIMITER}${NeoDataParser.LABEL_REL_DEP}")
          _depCount += 1
        }

    }
  }

  def parseDependencies(source: Source, outGav: PrintWriter, outDep: PrintWriter) = {
    logger.info("Parsing input file...")
    parseDependencyMap(source)
    logger.info("Writing output files...")
    writeDependencyMap(outGav, outDep)
  }

}
