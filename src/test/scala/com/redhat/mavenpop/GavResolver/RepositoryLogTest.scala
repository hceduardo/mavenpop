package com.redhat.mavenpop.GavResolver

import org.scalatest._

class RepositoryLogTest extends FlatSpec with Matchers {

  "parseLogLine" should "return None given  null argument" in {
    RepositoryLog.parseLogLine(null) should be (None)
  }

  it should "return None given empty string argument" in {
      RepositoryLog.parseLogLine("") should be (None)
  }

  it should "return None given line in wrong format" in {
    val line = "clientID-wrong 1421030666000 m2e junit/junit/4.8.1/junit-4.8.1.pom"
    RepositoryLog.parseLogLine(line) should be (None)
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
