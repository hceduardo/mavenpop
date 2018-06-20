package com.redhat.mavenpop.GavResolver

import org.scalatest._

class DependencyRecordTest extends FlatSpec with Matchers {

  "parseLogLine" should "throw RuntimeException for null argument" in {
    assertThrows[RuntimeException]{
      DependencyRecord.parseLogLine(null)
    }
  }

  it should "throw RuntimeException for empty string argument" in {
    assertThrows[RuntimeException]{
      DependencyRecord.parseLogLine("")
    }
  }

  it should "throw RuntimeException for line in wrong format" in {
    assertThrows[RuntimeException]{
      DependencyRecord.parseLogLine(
        """"406742 commons-logging/1.1.1/commons-logging-1.1.1.pom commons-logging:commons-logging:1.1.1 junit:junit:3.8.1,log4j:log4j:1.2.12 Additional Param"""")
    }
  }

  it should "correctly parse all log line fields" in {
    val dependencyRecord = DependencyRecord.parseLogLine(
      """406742 commons-logging/1.1.1/commons-logging-1.1.1.pom commons-logging:commons-logging:1.1.1 junit:junit:3.8.1,log4j:log4j:1.2.12"""
    ).get

    dependencyRecord.path should be ("""commons-logging/1.1.1/commons-logging-1.1.1.pom""")
    dependencyRecord.gav should be ("""commons-logging:commons-logging:1.1.1""")
    dependencyRecord.dependencies should be (Array("""junit:junit:3.8.1""","""log4j:log4j:1.2.12"""))
  }
}
