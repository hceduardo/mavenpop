package com.redhat.mavenpop.Reporter

import com.redhat.mavenpop.MavenPopJob

object ReporterJob extends MavenPopJob {

  def main(args: Array[String]) {

    val enhancedSessions = getEnhancedSessions

    Reporter.writeReport(spark, enhancedSessions, conf.reportDir)

    spark.stop()
  }

  private def getEnhancedSessions = {
    spark.read.parquet(conf.enhancedSessionsPath).
      filter(s"startTime >= ${conf.startTime}").
      filter(s"startTime < ${conf.endTime}")
  }
}
