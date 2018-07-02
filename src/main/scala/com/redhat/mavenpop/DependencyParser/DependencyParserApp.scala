package com.redhat.mavenpop.DependencyParser


import org.apache.log4j.LogManager

object DependencyParserApp {

  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("usage: neoDataParser INPUT_FILE OUTGAVFILE OUTDEPFILE")
      System.exit(1)
    }

    val inFilename = args(0)
    val outGavFilename = args(1)
    val outDepFilename = args(2)

    new NeoDataParser().parseDependencies(inFilename, outGavFilename, outDepFilename)
  }

}
