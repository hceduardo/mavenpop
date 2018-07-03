package com.redhat.mavenpop.GavResolver

import org.scalatest._

class DependencyRecordTest extends FlatSpec with Matchers {

  "parseLogLine" should "return None given null argument" in {

    DependencyRecord.parseLogLine(null) should be(None)

  }

  it should "return None given empty string argument" in {
    DependencyRecord.parseLogLine("") should be(None)
  }

  it should "return None given line in wrong format" in {

    val line = """"406742 commons-logging/1.1.1/commons-logging-1.1.1.pom commons-logging:commons-logging:1.1.1 junit:junit:3.8.1,log4j:log4j:1.2.12 Additional Param""""

    DependencyRecord.parseLogLine(line) should be(None)

  }

  it should "correctly parse all log line fields" in {
    val dependencyRecord = DependencyRecord.parseLogLine(
      """406742 commons-logging/1.1.1/commons-logging-1.1.1.pom commons-logging:commons-logging:1.1.1 junit:junit:3.8.1,log4j:log4j:1.2.12""").get

    dependencyRecord.path should be("""commons-logging/1.1.1/commons-logging-1.1.1.pom""")
    dependencyRecord.gav should be("""commons-logging:commons-logging:1.1.1""")
    dependencyRecord.dependencies should be(Array("""junit:junit:3.8.1""", """log4j:log4j:1.2.12"""))
  }
}
