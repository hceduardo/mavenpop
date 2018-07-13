package com.redhat.mavenpop

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object Experiment3 {

  val logger = LogManager.getLogger(this.getClass.getName)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Experiment")
      .master("local[*]")
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    spark.stop()
  }
}
