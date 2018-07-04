package com.redhat.mavenpop.Sessioniser

import com.redhat.mavenpop.MavenPopConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

object SessioniserJob {

  def main(args: Array[String]) {

    val conf: MavenPopConfig = new MavenPopConfig("mavenpop.conf")

    val sparkMaster = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder.appName("Sessioniser")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    val gavLogs = spark.read.parquet(conf.gavLogsPath)
    val sessions = Sessioniser.createSessions(spark, gavLogs, conf.sessionMaxIdleMillis)
    sessions.write.mode(SaveMode.Overwrite).parquet(conf.sessionsPath)

    spark.stop()
  }

}
