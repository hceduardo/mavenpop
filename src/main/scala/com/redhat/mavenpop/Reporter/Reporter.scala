package com.redhat.mavenpop.Reporter

import com.redhat.mavenpop.MavenPopConfig
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Reporter {

  val logger = LogManager.getLogger(getClass.getName)
  val conf = new MavenPopConfig()

  def writeReport(spark: SparkSession, sessionsWithDependencies: DataFrame, reportDir: String): Unit = {
    import spark.implicits._

    val getTopLevel = udf((gavs: Seq[String], dependencies: Seq[String]) => gavs.diff(dependencies))

    logger.info("calculating direct and indirect usage")

    val directIndirect = sessionsWithDependencies
      .withColumn("directSeq", getTopLevel($"gavs", $"dependencies"))
      .withColumnRenamed("dependencies", "indirectSeq")
      .persist()

    // Testing: gavs.size = indirect.size + direct.size
    // directIndirect.select(size($"gavs"), size($"indirectSeq"), size ($"directSeq") ).show

    val direct = directIndirect.select("clientId", "startTime", "endTime", "directSeq")
    val indirect = directIndirect.select("clientId", "startTime", "endTime", "indirectSeq")

    val directReport = direct.
      withColumn("direct", explode($"directSeq")).
      groupBy("direct").
      agg(countDistinct("clientId").as("clients")).
      orderBy(desc("clients"))

    val indirectReport = indirect.
      withColumn("indirect", explode($"indirectSeq")).
      groupBy("indirect").
      agg(countDistinct("clientId").as("clients")).
      orderBy(desc("clients"))


    val reportPrefix = reportDir + s"${conf.startTimeString}_${conf.endTimeString}_"
    val directPath = reportPrefix + "direct.csv"
    val indirectPath = reportPrefix + "indirect.csv"

    logger.info(s"Writing Reports to $reportPrefix*")

    directReport.write.mode(SaveMode.Overwrite).csv(directPath)
    indirectReport.write.mode(SaveMode.Overwrite).csv(indirectPath)

  }


}
