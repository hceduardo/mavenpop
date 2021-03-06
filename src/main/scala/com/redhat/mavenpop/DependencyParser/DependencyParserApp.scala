package com.redhat.mavenpop.DependencyParser

import java.io.{ PrintWriter, StringWriter }

import com.redhat.mavenpop.MavenPopConfig
import org.apache.log4j.LogManager

import scala.io.Source

object DependencyParserApp {

  private val logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val conf: MavenPopConfig = new MavenPopConfig()

    if (args.length != 3) {
      println("usage: neoDataParser INPUT_FILE OUTGAVFILE OUTDEPFILE")
      System.exit(1)
    }

    val inFilename = args(0)
    val outGavFilename = args(1)
    val outDepFilename = args(2)

    var source: Source = null
    var outGav: PrintWriter = null
    var outDep: PrintWriter = null

    var exceptionCaught = false

    try {
      source = Source.fromFile(inFilename)
      outGav = new PrintWriter(outGavFilename)
      outDep = new PrintWriter(outDepFilename)

      val parser = new NeoDataParser()

      parser.parseDependencies(source, outGav, outDep, conf.parserWriteTransitive)

      logger.info("Parsing finished successfully")
      logger.info(s"Generated ${parser.gavCount} unique gavs to ${outGavFilename}")
      logger.info(s"Generated ${parser.depCount} unique dependencies to ${outDepFilename}")

    } catch {
      //Todo: catch different exceptions and display specific error messages
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        logger.error(sw.toString)
        exceptionCaught = true
      }
    } finally {
      source.close()
      outGav.close()
      outDep.close()
      if (exceptionCaught) {
        System.exit(1)
      }
    }
  }

}
