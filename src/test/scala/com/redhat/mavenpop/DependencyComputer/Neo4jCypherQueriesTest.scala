/***
  * ref: https://neo4j.com/docs/java-reference/current/tutorials-java-embedded/#tutorials-java-unit-testing
  */

package com.redhat.mavenpop.DependencyComputer

import java.util

import com.univocity.parsers.common.ResultIterator
import org.scalatest._

import scala.collection.JavaConverters._
import org.neo4j.graphdb.{GraphDatabaseService, ResourceIterator, Result, Transaction}
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.test.rule.TestDirectory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class Neo4jCypherQueriesTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  protected val DependenciesPath: String = "/UsageAnalyser/dependencies.txt"

  protected var graphDb: GraphDatabaseService = null

  protected override def beforeEach(): Unit = {
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase(
      //TestDirectory.testDirectory().directory()
    )
    loadTestGraph
  }

  protected override def afterEach(): Unit = {
    graphDb.shutdown()
  }

  "neo4j" should "create correct graph" in {

    // Validate graph loaded correctly with node count and relationship count

    var tx: Transaction = null

    var nodeCount: Long = 0L
    var relCount: Long = 0L

    try{
      tx = graphDb.beginTx()
      var result: Result = null

      result = graphDb.execute(HelperQueries.CountNodes)

      result.hasNext should be (true)
      nodeCount = result.next().get("count").asInstanceOf[Long]

      result.close() // ignore additional results from previous query
      result = graphDb.execute(HelperQueries.CountRelationships)

      result.hasNext should be (true)
      relCount = result.next().get("count").asInstanceOf[Long]

      tx.success()
    }finally {
      if(tx != null) tx.close()
    }

    nodeCount should be (15L)
    relCount should be (21L)

  }

  "GetDependenciesFromList" should "return only dependencies" in {

    val inputStr: String = "mavenpop:test:top1,mavenpop:test:dep1,mavenpop:test:dep2,mavenpop:test:dep3,mavenpop:test:dep4,mavenpop:test:dep22,mavenpop:test:top2,mavenpop:test:dep5,mavenpop:test:dep6,mavenpop:test:dep23,mavenpop:test:top3,mavenpop:test:dep7,mavenpop:test:dep8,mavenpop:test:dep21,mavenpop:test:dep24"
    val expectedStr: String = "mavenpop:test:dep1,mavenpop:test:dep2,mavenpop:test:dep3,mavenpop:test:dep4,mavenpop:test:dep22,mavenpop:test:dep5,mavenpop:test:dep6,mavenpop:test:dep23,mavenpop:test:dep7,mavenpop:test:dep8,mavenpop:test:dep21,mavenpop:test:dep24"

    validateGetDependenciesFromList(inputStr, expectedStr)

  }

  it should "return empty given only top level dependencies" in {

    val inputStr: String = "mavenpop:test:top1,mavenpop:test:top2,mavenpop:test:top3"
    val expectedStr: String = ""

    validateGetDependenciesFromList(inputStr, expectedStr)

  }

  it should "return empty given all gavs not in graph" in {

    val inputStr: String = "mavenpop:test:absent1,mavenpop:test:absent2,mavenpop:test:absent3"
    val expectedStr: String = ""

    validateGetDependenciesFromList(inputStr, expectedStr)

  }

  it should "return empty given top level gavs and gavs not in graph" in {

    val inputStr: String = "mavenpop:test:top1,mavenpop:test:absent1,mavenpop:test:absent2,mavenpop:test:absent3"
    val expectedStr: String = ""

    validateGetDependenciesFromList(inputStr, expectedStr)

  }

  it should "return third level given top level gavs, third level dependencies and gavs not in graph" in {

    val inputStr: String = "mavenpop:test:absent1,mavenpop:test:top1,mavenpop:test:top2,mavenpop:test:top3,mavenpop:test:dep24"
    val expectedStr: String = "mavenpop:test:dep24"

    validateGetDependenciesFromList(inputStr, expectedStr)

  }

  private def loadTestGraph = {
    // Initialize parameters
    val fileURL: String = "file://" + getClass.getResource(DependenciesPath).getPath
    val parameters = new util.HashMap[String, Object]()
    parameters.put("fileURL", fileURL)

    // Load graph from sample dependencies file (fileURL)

    var tx: Transaction = null
    try {
      tx = graphDb.beginTx()
      graphDb.execute(HelperQueries.LoadNodes, parameters)
      graphDb.execute(HelperQueries.LoadRelationships, parameters)
      tx.success()
    } finally {
      if (tx != null) tx.close()
    }
  }

  private def validateGetDependenciesFromList(inputStr: String, expectedStr: String): Unit = {
    // Parameter initialization

    val inputGavs = inputStr.split(",").toSeq.asJava
    val parameters = Map[String, Object]("gavList" -> inputGavs).asJava

    // Declare transaction and result objects
    var tx: Transaction = null

    // Validate graph loaded correctly with node count and relationship count

    val dependencies = new ArrayBuffer[String]()

    try {
      tx = graphDb.beginTx()

      val queryResult = graphDb.execute(CypherQueries.GetDependenciesFromList, parameters)

      while (queryResult.hasNext()) {

        dependencies.append(queryResult.next().get("dependencyId").toString)
      }

      tx.success()
    } finally {
      if (tx != null) tx.close()
    }

    if(expectedStr == ""){
      dependencies shouldBe empty
    }else{
      val expectedGavs = expectedStr.split(",")
      expectedGavs.sorted should contain theSameElementsAs dependencies.sorted
    }
  }
}
