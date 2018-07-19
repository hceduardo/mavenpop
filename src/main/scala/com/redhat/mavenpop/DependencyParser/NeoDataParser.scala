package com.redhat.mavenpop.DependencyParser

import java.io.PrintWriter

import com.redhat.mavenpop.GavResolver.DependencyRecord
import com.redhat.mavenpop.MavenPopConfig

import scala.io.Source
import org.apache.log4j.LogManager

import scala.collection.mutable

object NeoDataParser {
  val DELIMITER = "|"
  val HEADER_NODE_GAV = s"id:ID$DELIMITER:LABEL"
  val HEADER_REL_DEP = s":START_ID$DELIMITER:END_ID$DELIMITER:TYPE"
}

class NeoDataParser {

  private var _gavCount: Integer = 0
  private var _depCount: Integer = 0

  def gavCount = _gavCount
  def depCount = _depCount

  private val dependencyMap = mutable.Map[String, scala.collection.mutable.Set[String]]()
  private val transitiveDependencyMap = mutable.Map[String, scala.collection.mutable.Set[String]]()
  private val excludedDeps = Set[String]("UNKNOWN_DEPS", "NO_DEPS")

  private val logger = LogManager.getLogger(getClass.getName)

  private val conf: MavenPopConfig = new MavenPopConfig()

  def parseDependencies(source: Source, outGav: PrintWriter, outDep: PrintWriter, writeTransitiveAsDirect: Boolean): Unit = {

    logger.info("Parsing input file...")
    parseDependencyMap(source) //populate dependencyMap

    if (writeTransitiveAsDirect) {

      logger.info("generating transitive dependency map")
      buildTransitiveDependencyMap()

      logger.info("Writing output files with transitive dependencies...")
      writeDependencyMap(transitiveDependencyMap.iterator,
        conf.gavLabel, conf.transitiveDepLabel, outGav, outDep)

    } else {

      logger.info("Writing output files with direct dependencies...")
      writeDependencyMap(dependencyMap.iterator,
        conf.gavLabel, conf.directDepLabel, outGav, outDep)

    }

  }

  def parseDependencies(source: Source, outGav: PrintWriter, outDep: PrintWriter): Unit =
    parseDependencies(source, outGav, outDep, false)


  private def addDirectDep(gav: String, dependencies: Set[String]): Unit = {

    // Do not add UNKNOWN_DEPS or NO_DEPS nodes
    if (dependencies.size == 1 && excludedDeps.contains(dependencies.toSeq(0))) {
      return
    }

    val _deps = dependencies -- excludedDeps

    _deps.foreach(dep => {
      if (!dependencyMap.contains(dep)) {
        dependencyMap(dep) = mutable.Set.empty[String]
      }
    })

    dependencyMap.contains(gav) match {
      case false => dependencyMap(gav) = mutable.Set(_deps.toSeq: _*)
      case true => dependencyMap(gav) ++= _deps
    }
  }

  private def parseDependencyMap(source: Source): Unit = {

    var lineNumber = 0

    source.getLines.foreach(line => {

      lineNumber += 1
      DependencyRecord.parseLogLine(line) match {
        case None => logger.warn(s"could not parse line ${lineNumber}: ${line}")
        case Some(dependencyRecord) => {
          addDirectDep(dependencyRecord.gav, dependencyRecord.dependencies.toSet)
        }
      }

    })
  }

  private def writeDependencyMap( mapEntries: Iterator[(String, mutable.Set[String])],
                                  nodeLabel: String, relationshipLabel: String,
                                  outGav: PrintWriter, outDep: PrintWriter
                                  ) = {
    assert(!dependencyMap.isEmpty)

    outGav.println(NeoDataParser.HEADER_NODE_GAV)
    outDep.println(NeoDataParser.HEADER_REL_DEP)

    mapEntries.toSeq.sortBy(n => n._1).foreach {
      case (gav: String, dependencies: mutable.Set[String]) =>

        outGav.println(s"${gav}${NeoDataParser.DELIMITER}${nodeLabel}")
        _gavCount += 1

        dependencies.foreach { dep =>
          outDep.println(s"${gav}${NeoDataParser.DELIMITER}${dep}${NeoDataParser.DELIMITER}${relationshipLabel}")
          _depCount += 1
        }

    }
  }

  private def buildTransitives(gav: String, visited: mutable.Set[String]): mutable.Set[String] = {
    if (transitiveDependencyMap.contains(gav)) {
      return transitiveDependencyMap(gav)
    }

    visited.add(gav)

    val transitives = mutable.Set.empty[String]

    if (dependencyMap.contains(gav)) {
      val directs = dependencyMap(gav)
      transitives ++= directs

      directs.foreach { direct: String =>
        if (!visited.contains(direct)) {
          visited += direct
          transitives ++= buildTransitives(direct, visited)
        }
      }

    }

    transitiveDependencyMap(gav) = transitives
    return transitives

  }

  private def buildTransitiveDependencyMap() = {
    dependencyMap.iterator.foreach {
      case (gav: String, _) =>
        buildTransitives(gav, mutable.Set.empty[String])
    }
  }

}
