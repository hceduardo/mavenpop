package com.redhat.mavenpop.GavResolver

import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest._


class ParserTest extends FlatSpec with Matchers with SharedSparkSession {

  val repologPath = "/GavResolver/repolog-sample.txt"
  val dependenciesPath = "/GavResolver/dependencies-sample.txt"
  val duplicatedDepPath = "/dependencies-sample-duplicated.txt"

  "parseRepositoryLogs" should "import only correctly formatted lines" in {

    val repologs = Parser.parseRepositoryLogs(spark,
      getClass.getResource(repologPath).getPath)

    repologs.count() should be (2)

  }

  "parseDependencyRecords" should "import only correctly formatted lines" in {
    val dependencyRecords = Parser.parseDependencyRecords(spark,
      getClass.getResource(dependenciesPath).getPath)

    dependencyRecords.count() should be (2)
  }

  it should "throw RuntimeException given file with duplicated gavs" in {

    assertThrows[RuntimeException] {
      Parser.parseDependencyRecords(spark,
        getClass.getResource(duplicatedDepPath).getPath)
    }

  }

}
