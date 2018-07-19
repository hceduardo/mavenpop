package com.redhat.mavenpop.Reporter

import com.redhat.mavenpop.MavenPopJob
import org.apache.spark.sql.SaveMode

object ReporterJob extends MavenPopJob {

  def main(args: Array[String]) {

    val sessionsWithDependencies = spark.read.parquet(conf.sessionsWithDepsPath)

    Reporter.writeReport(spark,sessionsWithDependencies, conf.reportDir)

    spark.stop()
  }

}
