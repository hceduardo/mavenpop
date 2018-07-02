package com.redhat.mavenpop.DependencyParser

import java.io.{PrintWriter, StringWriter}

import scala.io.Source

object DependencyParserApp {

  private val NodeLabelGav = "GAV"
  private val RelLabelDep = "DEPENDS_ON"

  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("usage: neoDataParser INPUT_FILE OUTGAVFILE OUTDEPFILE")
      System.exit(1)
    }

    val inFilename = args(0)
    val outGavFilename = args(1)
    val outDepFilename = args(2)

    var source: Source = null
    var outGav: PrintWriter = null
    var outDep: PrintWriter = null

    var gavCount: Integer = 0
    var depCount: Integer = 0
    var exceptionCatched = false

    try{
      source = Source.fromFile(inFilename)
      outGav = new PrintWriter(outGavFilename)
      outDep = new PrintWriter(outDepFilename)

      outGav.println("id:ID,:LABEL")
      outDep.println(":START_ID,:END_ID,:TYPE")

      var lineNumber = 0

      source.getLines.foreach{ line =>

        lineNumber += 1

        try{
          val Array(count, path, gav, depString) = line.split(" ").map(_.trim)
          val dependencies = depString.split(",").map(_.trim)

          outGav.println(s"${gav},${NodeLabelGav}")
          gavCount += 1

          for(dep <- dependencies){
            outGav.println(s"${dep},${NodeLabelGav}")
            outDep.println(s"${gav},${dep},${RelLabelDep}")
            gavCount += 1
            depCount += 1
          }
        } catch {
          case e:MatchError => {
            println(s"[NeoDataParser] WARN: could not parse line ${lineNumber}: ${line}")
          }
        }
      }
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
      if(exceptionCatched){
        System.exit(1)
      }
    }
  }

}
