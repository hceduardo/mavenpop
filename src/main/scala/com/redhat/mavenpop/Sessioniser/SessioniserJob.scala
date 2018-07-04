package com.redhat.mavenpop.Sessioniser

import com.redhat.mavenpop.MavenPopConfig
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ SaveMode, SparkSession }

object SessioniserJob {

  private val logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]) {

    // Load resources/reference.conf by default
    // Allows override with -Dconfig.file=path/to/config-file
    val conf: MavenPopConfig = new MavenPopConfig()

    logger.info(conf)

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
