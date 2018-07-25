package com.redhat.mavenpop

import com.redhat.mavenpop.DependencyComputer.DependencyComputerProfilerJob.logger
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SparkSession

trait MavenPopJob {

  // Load resources/reference.conf by default
  // Allows override with -Dconfig.file=path/to/config-file
  protected val conf: MavenPopConfig = new MavenPopConfig()

  private val jobName = this.getClass.getSimpleName

  protected val logger: Logger = LogManager.getLogger(this.getClass.getName)

  logger.info(s"starting $jobName")

/***
    * session should be stopped at the end of the implementing class main method with spark.stop()
    */
  protected val spark: SparkSession = SparkSession.builder.appName(jobName)
    .master(conf.sparkMasterUrl)
    .config("spark.eventLog.enabled", conf.sparkEnableEventLog)
    .getOrCreate()
}
