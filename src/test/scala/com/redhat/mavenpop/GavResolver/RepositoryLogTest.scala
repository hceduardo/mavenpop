package com.redhat.mavenpop.GavResolver

import org.scalatest._

class RepositoryLogTest extends FlatSpec with Matchers {

  "parseLogLine" should "throw RuntimeException for null argument" in {
    assertThrows[RuntimeException]{
      RepositoryLog.parseLogLine(null)
    }
  }

  it should "throw RuntimeException for empty string argument" in {
    assertThrows[RuntimeException]{
      RepositoryLog.parseLogLine("")
    }
  }

  it should "throw RuntimeException for line in wrong format" in {
    assertThrows[RuntimeException]{
      RepositoryLog.parseLogLine(
        "clientID-wrong 1421030666000 m2e junit/junit/4.8.1/junit-4.8.1.pom")
    }
  }

  it should "correctly parse all log line fields" in {
    val repositoryLog = RepositoryLog.parseLogLine(
      """79 1421030666000 m2e junit/junit/4.8.1/junit-4.8.1.pom"""
    ).get

    repositoryLog.clientId should be (79)
    repositoryLog.timestamp should be (1421030666000L)
    repositoryLog.agent should be ("m2e")
    repositoryLog.path should be ("""junit/junit/4.8.1/junit-4.8.1.pom""")
  }
}
