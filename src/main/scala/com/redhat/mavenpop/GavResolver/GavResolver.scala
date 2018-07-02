package com.redhat.mavenpop.GavResolver

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.asc

object GavResolver {
  def resolveGavs(
                   spark: SparkSession,
                   repositoryLogs: Dataset[RepositoryLog], dependencyRecords: Dataset[DependencyRecord]): Dataset[Row] = {

    import spark.implicits._

    val gavLogs = repositoryLogs.as("r").join(dependencyRecords.as("d"), $"r.path" === $"d.path").
      select("clientId", "timestamp", "agent", "gav", "dependencies").
      orderBy(asc("clientId"), asc("timestamp"))

    gavLogs

  }

}
