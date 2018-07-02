/***
  * ref: https://github.com/neo4j-contrib/neo4j-jdbc/blob/master/neo4j-jdbc-bolt/src/test/java/org/neo4j/jdbc/bolt/SampleIT.java
  * https://github.com/neo4j-contrib/neo4j-jdbc/blob/b6e26650785a3d6734cb1f20fe920d9f0c2f8a15/neo4j-jdbc-bolt/src/test/java/org/neo4j/jdbc/bolt/Neo4jBoltRule.java
  * https://gist.github.com/michaelahlers/461c95981a0a2ae346567e5a1ae7a5e7
  * https://github.com/neo4j-contrib/neo4j-jdbc/blob/master/neo4j-jdbc-bolt/src/test/java/org/neo4j/jdbc/bolt/Neo4jBoltRule.java
  *
  */

package com.redhat.mavenpop.DependencyComputer

import java.net.{InetSocketAddress, ServerSocket}

import org.neo4j.driver.v1._
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel._
import org.neo4j.kernel.configuration.Connector.ConnectorType._
import org.neo4j.kernel.configuration._
import org.neo4j.test._
import org.scalatest._
import java.util
import java.util.Arrays

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, WrappedArray}
import scala.collection.JavaConverters._

class Neo4JDependencyComputerTest extends FlatSpec with Matchers with BeforeAndAfterEach with SharedSparkSession {


  @inline private final val startTime = 1529598557000L // Thu 21 Jun 17:29:17 BST 2018 in epoch milliseconds
  @inline private final val endTime = startTime + 1000 // t0 + 1 second

  protected val DependenciesPath: String = "/UsageAnalyser/dependencies.txt"

  protected var graphDb: GraphDatabaseService = null

  private val host: String = "localhost"
  val port: Int = new ServerSocket(0).getLocalPort
  private val hostAndPort: String = s"$host:$port"
  private val boltUrl: String = s"bolt://$hostAndPort"
  private val driverConfig = org.neo4j.driver.v1.Config.build().withoutEncryption().toConfig

  protected override def beforeEach(): Unit = {

    val connector = new BoltConnector("(bolt-tests)")

    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
      .setConfig(connector.`type`, BOLT.name())
      .setConfig(connector.enabled, "true")
      .setConfig(GraphDatabaseSettings.auth_enabled, "false")
      .setConfig(connector.encryption_level, DISABLED.name())
      .setConfig(connector.listen_address, hostAndPort)
      .newGraphDatabase()

    loadTestGraph

  }

  protected override def afterEach(): Unit = {
    graphDb.shutdown()
  }

  "neo4j-driver" should "connect with any credentials given database auth disabled" in {
    val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic("any", "any"), driverConfig)
    val session = driver.session()

    session.run(HelperQueries.CountNodes)

    session.close()
    driver.close()
  }

  "computeDependencies" should "add only dependencies" in {

    val inputStr: String = "mavenpop:test:top1,mavenpop:test:dep1,mavenpop:test:dep2,mavenpop:test:dep3,mavenpop:test:dep4,mavenpop:test:dep22,mavenpop:test:top2,mavenpop:test:dep5,mavenpop:test:dep6,mavenpop:test:dep23,mavenpop:test:top3,mavenpop:test:dep7,mavenpop:test:dep8,mavenpop:test:dep21,mavenpop:test:dep24"
    val expectedStr: String = "mavenpop:test:dep1,mavenpop:test:dep2,mavenpop:test:dep3,mavenpop:test:dep4,mavenpop:test:dep22,mavenpop:test:dep5,mavenpop:test:dep6,mavenpop:test:dep23,mavenpop:test:dep7,mavenpop:test:dep8,mavenpop:test:dep21,mavenpop:test:dep24"

    validateComputeDependencies(inputStr, expectedStr)

  }

  it should "add empty dependencies given: only top level dependencies" in {

    val inputStr: String = "mavenpop:test:top1,mavenpop:test:top2,mavenpop:test:top3"
    val expectedStr: String = ""

    validateComputeDependencies(inputStr, expectedStr)

  }

  it should "add empty dependencies list given: all gavs not in graph" in {

    val inputStr: String = "mavenpop:test:absent1,mavenpop:test:absent2,mavenpop:test:absent3"
    val expectedStr: String = ""

    validateComputeDependencies(inputStr, expectedStr)

  }

  it should "add empty dependencies given: top level gavs and gavs not in graph" in {

    val inputStr: String = "mavenpop:test:top1,mavenpop:test:absent1,mavenpop:test:absent2,mavenpop:test:absent3"
    val expectedStr: String = ""

    validateComputeDependencies(inputStr, expectedStr)

  }

  it should "add third level dependencies given: top level gavs, third level dependencies and gavs not in graph" in {

    val inputStr: String = "mavenpop:test:absent1,mavenpop:test:top1,mavenpop:test:top2,mavenpop:test:top3,mavenpop:test:dep24"
    val expectedStr: String = "mavenpop:test:dep24"

    validateComputeDependencies(inputStr, expectedStr)

  }
  
  private def validateComputeDependencies(inputStr: String, expectedStr: String) = {
    val inputSession = createSession(inputStr)
    val expectedDf = createSessionWithDeps(inputStr, expectedStr)

    val sessionAnalyser = new Neo4JDependencyComputer(boltUrl, "any", "any", true)
    val actualDf = sessionAnalyser.computeDependencies(spark, inputSession)

    areDataFramesEqual(actualDf, expectedDf) should be(true)
  }

  /** *
    * expects both dataframes to be
    * StructType(List(
      *StructField("clientId",IntegerType,false),
      *StructField("sessionId",LongType,true),
      *StructField("startTime",LongType,true),
      *StructField("endTime",LongType,true),
      *StructField("gavs",ArrayType(StringType,true),true),
      *StructField("dependencies",ArrayType(StringType,true),true)
    */
  private def areDataFramesEqual(df1: DataFrame, df2: DataFrame): Boolean = {

    // ToDo: for multiline dataframes: sort both dataframes by all columns first. Consider using a Spark Testing framework

    // Need to order the session array ("gavs" column contents) for comparision to succeed
    val sortArrayUDF = udf[WrappedArray[String], WrappedArray[String]] { _.sorted}

    // Generate sessions and order session contents
    val sortedDf1 = df1.withColumn("gavs",sortArrayUDF(df1.col("gavs"))).
      withColumn("dependencies",sortArrayUDF(df1.col("dependencies")))

    val sortedDf2 = df2.withColumn("gavs",sortArrayUDF(df2.col("gavs"))).
      withColumn("dependencies",sortArrayUDF(df2.col("dependencies")))
    //orderedActualSessions.collect().sameElements(orderedExpectedSessions.collect) should be (true)

    val a1 = sortedDf1.collect
    val a2 = sortedDf2.collect

    val sameElements = a1.sameElements(a2)

    if(!sameElements){
      print(generateMismatchMessage(a1, a2))
    }

    return sameElements

  }

  private def generateMismatchMessage[T](a1: Array[T], a2: Array[T]): String = {
    if(a1.size != a2.size){
      s"different sizes: ${a1.size} != ${a2.size}"
    }
    else {
      "different elements: \n" + a1.zip(a2).flatMap {
        case (r1, r2) =>
          if (!r1.equals(r2)) {
            Some(s"$r1 | $r2")
          } else {
            None
          }
      }.mkString("\n")
    }
  }

  private def createSessionWithDeps(sessionStr: String, dependenciesStr: String) :DataFrame ={

    val sessionsWithDepSchema = StructType(List(
      StructField("clientId",IntegerType,false),
      StructField("sessionId",LongType,true),
      StructField("startTime",LongType,true),
      StructField("endTime",LongType,true),
      StructField("gavs",ArrayType(StringType,true),true),
      StructField("dependencies",ArrayType(StringType,true),true)
    ))

    val dependenciesArr = if (dependenciesStr == "") new Array[String](0)
                          else dependenciesStr.split(",")

    val sessionsWithDepData = Arrays.asList(
      Row(1, 0L, startTime, endTime, sessionStr.split(","), dependenciesArr)
    )

    val sessionsWithDep = spark.createDataFrame(sessionsWithDepData, sessionsWithDepSchema)

    sessionsWithDep

  }

  private def createSession(inputStr: String): DataFrame = {

    val sessionsSchema = StructType(List(
      StructField("clientId",IntegerType,false),
      StructField("sessionId",LongType,true),
      StructField("startTime",LongType,true),
      StructField("endTime",LongType,true),
      StructField("gavs",ArrayType(StringType,true),true)
    ))

    val sessionsData = Arrays.asList(
      Row(1, 0L, startTime, endTime, inputStr.split(","))
    )

    val sessions = spark.createDataFrame(sessionsData, sessionsSchema)

    sessions
  }

  private def loadTestGraph = {
    // Initialize parameters
    val fileURL: String = "file://" + getClass.getResource(DependenciesPath).getPath
    val parameters = new util.HashMap[String, Object]()
    parameters.put("fileURL", fileURL)

    // Load graph from sample dependencies file (fileURL)

    var tx: org.neo4j.graphdb.Transaction = null
    try {
      tx = graphDb.beginTx()
      graphDb.execute(HelperQueries.LoadNodes, parameters)
      graphDb.execute(HelperQueries.LoadRelationships, parameters)
      tx.success()
    } finally {
      if (tx != null) tx.close()
    }
  }
}
