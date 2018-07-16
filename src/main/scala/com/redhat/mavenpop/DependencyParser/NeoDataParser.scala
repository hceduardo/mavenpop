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
  private val dependencyMap = new DependencyMap()

  private val transitiveDependencyMap = mutable.Map[String, scala.collection.mutable.Set[String]]()

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

  private def writeDependencyMap(outGav: PrintWriter, outDep: PrintWriter,
                                 mapEntries: Iterator[(String, mutable.Set[String])]) = {
    assert(!dependencyMap.isEmpty)

    outGav.println(NeoDataParser.HEADER_NODE_GAV)
    outDep.println(NeoDataParser.HEADER_REL_DEP)

    mapEntries.foreach {
      case (gav: String, dependencies: mutable.Set[String]) =>

        outGav.println(s"${gav}${NeoDataParser.DELIMITER}${NeoDataParser.LABEL_NODE_GAV}")
        _gavCount += 1

        dependencies.foreach { dep =>
          outDep.println(s"${gav}${NeoDataParser.DELIMITER}${dep}${NeoDataParser.DELIMITER}${NeoDataParser.LABEL_REL_DEP}")
          _depCount += 1
        }

    }
  }

  private def buildTransitives(gav: String, visited: mutable.Set[String]): mutable.Set[String] ={
    if(transitiveDependencyMap.contains(gav)){
      return transitiveDependencyMap(gav)
    }

    visited.add(gav)

    val transitives = mutable.Set.empty[String]

    if(dependencyMap.contains(gav)){
      val directs = dependencyMap(gav)
      transitives ++= directs

      directs.foreach { direct: String =>
        if(!visited.contains(direct)){
          visited += direct
          transitives ++= buildTransitives(direct, visited)
        }
      }

    }

    transitiveDependencyMap(gav) = transitives
    return transitives

  }

  private def buildTransitiveDependencyMap()={
    dependencyMap.iterator.foreach{ case (gav: String, _) =>
      buildTransitives(gav, mutable.Set.empty[String])
    }
  }

  def parseDependencies(source: Source, outGav: PrintWriter, outDep: PrintWriter, writeTransitiveAsDirect: Boolean): Unit = {
    logger.info("Parsing input file...")
    parseDependencyMap(source) //populate dependencyMap

    if(!writeTransitiveAsDirect){
      logger.info("Writing output files...")
      writeDependencyMap(outGav, outDep, dependencyMap.iterator)
      return
    }

    logger.info("generating transitive dependency map")
    buildTransitiveDependencyMap()

    logger.info("Writing output files...")
    writeDependencyMap(outGav, outDep, transitiveDependencyMap.iterator)

  }

  def parseDependencies(source: Source, outGav: PrintWriter, outDep: PrintWriter): Unit =
    parseDependencies(source, outGav, outDep, false)

}
