package com.redhat.mavenpop.UsageAnalyser

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SessionBuilder {

  def createSessions(spark: SparkSession, gavLogs: DataFrame, maxIdleMilliseconds: Long): DataFrame = {
    import spark.implicits._

    val partitionWindow = Window.partitionBy("clientId").orderBy("clientId", "timestamp")

    val gavLogsWithSessId: DataFrame = gavLogs.
      withColumn("prev", lag("timestamp", 1).over(partitionWindow)).
      withColumn("diff", $"timestamp" - $"prev").
      withColumn(
        "isNewSession",
        when($"diff" > lit(maxIdleMilliseconds), lit(1)).otherwise(lit(0))).
        withColumn(
          "sessionId",
          sum($"isNewSession").over(partitionWindow)).
          select("clientId", "timestamp", "gav", "sessionId")

    val sessions = gavLogsWithSessId.groupBy("clientId", "sessionId").
      agg(
        min("timestamp").as("startTime"),
        max("timestamp").as("endTime"),
        collect_set("gav").as("gavs"))

    sessions
  }

}
