/***
  * ref: https://github.com/neo4j-contrib/neo4j-jdbc/blob/master/neo4j-jdbc-bolt/src/test/java/org/neo4j/jdbc/bolt/SampleIT.java
  * https://github.com/neo4j-contrib/neo4j-jdbc/blob/b6e26650785a3d6734cb1f20fe920d9f0c2f8a15/neo4j-jdbc-bolt/src/test/java/org/neo4j/jdbc/bolt/Neo4jBoltRule.java
  * https://gist.github.com/michaelahlers/461c95981a0a2ae346567e5a1ae7a5e7
  * https://github.com/neo4j-contrib/neo4j-jdbc/blob/master/neo4j-jdbc-bolt/src/test/java/org/neo4j/jdbc/bolt/Neo4jBoltRule.java
  *
  */

package com.redhat.mavenpop.UsageAnalyser

import java.net.ServerSocket
import java.util

import org.neo4j.driver.v1._
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel._
import org.neo4j.kernel.configuration.Connector.ConnectorType._
import org.neo4j.kernel.configuration._
import org.neo4j.test._
import org.scalatest._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class Neo4jSessionAnalyserDriverTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  protected val DependenciesPath: String = "/UsageAnalyser/dependencies.txt"

  protected var graphDb: GraphDatabaseService = null
  protected var driver: Driver = null
  protected var session: Session = null

  val port: Int = new ServerSocket(0).getLocalPort

  protected override def beforeEach(): Unit = {

    val host: String = "localhost"

    val hostAndPort: String = s"$host:$port"
    val boltUrl: String = s"bolt://$hostAndPort"
    val driverConfig = org.neo4j.driver.v1.Config.build().withoutEncryption().toConfig

    val connector = new BoltConnector("(bolt-tests)")

    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
      .setConfig(connector.`type`, BOLT.name())
      .setConfig(connector.enabled, "true")
      .setConfig(GraphDatabaseSettings.auth_enabled, "false")
      .setConfig(connector.encryption_level, DISABLED.name())
      .setConfig(connector.listen_address, hostAndPort)
      .newGraphDatabase()

    loadTestGraph

    driver = GraphDatabase.driver(boltUrl, driverConfig)
    session = driver.session()
  }

  protected override def afterEach(): Unit = {
    session.close()
    driver.close()
    graphDb.shutdown()
  }

  "neo4jDriver" should "connect to database" in {
    // Connection is handled in beforeEach and afterEach
    // Only need to execute a simple statement in this test

    session.run(HelperQueries.CountNodes)
  }

  it should "retrieve graph statistics" in {

    var result: StatementResult = session.run(HelperQueries.CountNodes)
    result.hasNext should be (true)
    val nodeCount = result.next.get("count").asLong
    result.consume()

    result = session.run(HelperQueries.CountRelationships)
    result.hasNext should be (true)
    val relCount = result.next.get("count").asLong

    nodeCount should be (15L)
    relCount should be (21L)

  }

  it should "return only dependencies" in {

    val inputStr: String = "mavenpop:test:top1,mavenpop:test:dep1,mavenpop:test:dep2,mavenpop:test:dep3,mavenpop:test:dep4,mavenpop:test:dep22,mavenpop:test:top2,mavenpop:test:dep5,mavenpop:test:dep6,mavenpop:test:dep23,mavenpop:test:top3,mavenpop:test:dep7,mavenpop:test:dep8,mavenpop:test:dep21,mavenpop:test:dep24"
    val expectedStr: String = "mavenpop:test:dep1,mavenpop:test:dep2,mavenpop:test:dep3,mavenpop:test:dep4,mavenpop:test:dep22,mavenpop:test:dep5,mavenpop:test:dep6,mavenpop:test:dep23,mavenpop:test:dep7,mavenpop:test:dep8,mavenpop:test:dep21,mavenpop:test:dep24"

    validateGetDependenciesFromDriver(inputStr, expectedStr)

  }

  it should "return empty given only top level dependencies" in {

    val inputStr: String = "mavenpop:test:top1,mavenpop:test:top2,mavenpop:test:top3"
    val expectedStr: String = ""

    validateGetDependenciesFromDriver(inputStr, expectedStr)

  }

  it should "return empty given all gavs not in graph" in {

    val inputStr: String = "mavenpop:test:absent1,mavenpop:test:absent2,mavenpop:test:absent3"
    val expectedStr: String = ""

    validateGetDependenciesFromDriver(inputStr, expectedStr)

  }

  it should "return empty given top level gavs and gavs not in graph" in {

    val inputStr: String = "mavenpop:test:top1,mavenpop:test:absent1,mavenpop:test:absent2,mavenpop:test:absent3"
    val expectedStr: String = ""

    validateGetDependenciesFromDriver(inputStr, expectedStr)

  }

  it should "return third level given top level gavs, third level dependencies and gavs not in graph" in {

    val inputStr: String = "mavenpop:test:absent1,mavenpop:test:top1,mavenpop:test:top2,mavenpop:test:top3,mavenpop:test:dep24"
    val expectedStr: String = "mavenpop:test:dep24"

    validateGetDependenciesFromDriver(inputStr, expectedStr)

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

  private def validateGetDependenciesFromDriver(inputStr: String, expectedStr: String): Unit = {

    // Parameter initialization

    val inputGavs = inputStr.split(",").toSeq.asJava
    val parameters = Map[String, Object]("gavList" -> inputGavs).asJava

    // Validate graph loaded correctly with node count and relationship count

    //Todo: after unit testing the whole computeDependencies method, refactor Neo4jSessionAnalyser to call a protected function inside this unit test

    val dependencies = new ArrayBuffer[String]()

    val queryResult: StatementResult = session.run(CypherQueries.GetDependenciesFromList, parameters)

    while (queryResult.hasNext()) {

      dependencies.append(queryResult.next().get("dependencyId").asString())
    }

    if(expectedStr == ""){
      dependencies shouldBe empty
    }else{
      val expectedGavs = expectedStr.split(",")
      expectedGavs.sorted should contain theSameElementsAs dependencies.sorted
    }
  }
}
