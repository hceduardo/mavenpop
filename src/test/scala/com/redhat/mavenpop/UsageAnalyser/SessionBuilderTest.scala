package com.redhat.mavenpop.UsageAnalyser

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import java.util.Arrays

import org.scalatest._

import scala.collection.mutable.WrappedArray


class SessionBuilderTest extends FlatSpec with Matchers with SharedSparkSession {

  private val maxIdle = 1 * 60 * 1000 // Session threshold: one minute in millis
  private val t0 = 1529598557000L // Thu 21 Jun 17:29:17 BST 2018 in epoch milliseconds
  private val t1 = t0 + 1000 // t0 + 1 second
  private val t2 = t1 + maxIdle + 1 // t1 + above session threshold (force new session)
  private val t3 = t2 + 1000

  "createSessions" should "create correct sessions" in {

    // Need to order the session array ("gavs" column contents) for comparision to succeed

    val sortArrayUDF = udf[WrappedArray[String], WrappedArray[String]] { _.sorted}

    // Create Sample Gav Logs
    val gavLogs :DataFrame = createGavLogs(maxIdle)

    // Generate sessions and order session contents
    val actualSessions :DataFrame = SessionBuilder.createSessions(spark, gavLogs, maxIdle )
    val orderedActualSessions = actualSessions.withColumn("gavs",
      sortArrayUDF(actualSessions.col("gavs")))

    val expectedSessions :DataFrame = createExpectedSessions()
    val orderedExpectedSessions = expectedSessions.withColumn("gavs",
      sortArrayUDF(expectedSessions.col("gavs")))

    //orderedActualSessions.collect().sameElements(orderedExpectedSessions.collect) should be (true)

    orderedActualSessions.collect should contain theSameElementsAs orderedExpectedSessions.collect
  }

  /**
    * Creates gavLogs with two clients that would generate two sessions for each client
    * @param maxIdle threshold to build sessions
    * @return
    */
  private def createGavLogs(maxIdle: Int): DataFrame ={
    val gavLogsSchema = StructType(List(
      StructField("clientId",IntegerType,false),
      StructField("timestamp",LongType,false),
      StructField("agent",StringType,false),
      StructField("gav",StringType,false),
      StructField("dependencies",ArrayType(StringType,false),false)
    ))

    val gavLogsData = Arrays.asList(
      Row(1, t0 , "Nexus", "c1:s0:g1", Array("UNKNOWN")),
      Row(1, t1 , "Nexus", "c1:s0:g2", Array("UNKNOWN")),
      Row(1, t2 , "Nexus", "c1:s1:g1", Array("UNKNOWN")),
      Row(1, t3 , "Nexus", "c1:s1:g2", Array("UNKNOWN")),
      Row(2, t0 , "Nexus", "c2:s0:g1", Array("UNKNOWN")),
      Row(2, t1 , "Nexus", "c2:s0:g2", Array("UNKNOWN")),
      Row(2, t2 , "Nexus", "c2:s1:g1", Array("UNKNOWN")),
      Row(2, t3 , "Nexus", "c2:s1:g2", Array("UNKNOWN"))
    )

    val gavLogs = spark.createDataFrame(
      gavLogsData,
      gavLogsSchema
    )

    gavLogs
  }

  private def createExpectedSessions(): DataFrame = {

    val sessionsSchema = StructType(List(
      StructField("clientId",IntegerType,false),
      StructField("sessionId",LongType,true),
      StructField("startTime",LongType,true),
      StructField("endTime",LongType,true),
      StructField("gavs",ArrayType(StringType,true),true)
    ))


    val sessionsData = Arrays.asList(
      Row(1, 0L, t0, t1, Array("c1:s0:g1","c1:s0:g2")),
      Row(1, 1L, t2, t3, Array("c1:s1:g1","c1:s1:g2")),
      Row(2, 0L, t0, t1, Array("c2:s0:g1","c2:s0:g2")),
      Row(2, 1L, t2, t3, Array("c2:s1:g1","c2:s1:g2"))
    )

    val sessions = spark.createDataFrame(
      sessionsData,
      sessionsSchema
    )

    sessions
  }
}
