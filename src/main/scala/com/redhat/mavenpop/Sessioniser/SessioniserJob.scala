package com.redhat.mavenpop.Sessioniser

import com.redhat.mavenpop.MavenPopJob
import org.apache.spark.sql.SaveMode

object SessioniserJob extends MavenPopJob {

  def main(args: Array[String]) {

    val gavLogs = spark.read.parquet(conf.gavLogsPath)
    val sessions = Sessioniser.createSessions(spark, gavLogs, conf.sessionMaxIdleMillis)
    sessions.write.mode(SaveMode.Overwrite).parquet(conf.sessionsPath)

    logger.info(s"stopping $jobName")
    spark.stop()
  }

}
