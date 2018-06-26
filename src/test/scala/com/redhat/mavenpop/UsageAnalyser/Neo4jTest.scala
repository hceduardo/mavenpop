/***
  * ref: https://neo4j.com/docs/java-reference/current/tutorials-java-embedded/#tutorials-java-unit-testing
  */

package com.redhat.mavenpop.UsageAnalyser

import java.util

import com.univocity.parsers.common.ResultIterator
import org.scalatest._

import scala.collection.JavaConverters._
import org.neo4j.graphdb.{GraphDatabaseService, ResourceIterator, Result, Transaction}
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.test.rule.TestDirectory

import scala.collection.mutable.ArrayBuffer

class Neo4jTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  protected val DependenciesPath: String = "/UsageAnalyser/dependencies.txt"

  protected var graphDb: GraphDatabaseService = null

  protected override def beforeAll(): Unit = {
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase(
      //TestDirectory.testDirectory().directory()
    )
  }

  protected override def afterAll(): Unit = {
    graphDb.shutdown()
  }

  "neo4j" should "create graph" in {

    // Initialize parameters
    val fileURL: String = "file://" + getClass.getResource(DependenciesPath).getPath
    val parameters = new util.HashMap[String,Object]()
    parameters.put("fileURL", fileURL )

    // Declare transaction and result objects
    var tx: Transaction = null
    var result: Result = null

    // Load graph from sample dependencies file (fileURL)

    try{
      tx = graphDb.beginTx()
      graphDb.execute(CypherTestQueries.LoadNodes, parameters)
      graphDb.execute(CypherTestQueries.LoadRelationships, parameters)
      tx.success()
    }finally {
      if(tx != null) tx.close()
    }

    // Validate graph loaded correctly with node count and relationship count

    var nodeCount: Long = 0L
    var relCount: Long = 0L

    try{
      tx = graphDb.beginTx()
      result = graphDb.execute(CypherTestQueries.CountNodes)

      result.hasNext should be (true)
      nodeCount = result.next().get("count").asInstanceOf[Long]

      result.close() // ignore additional results from previous query
      result = graphDb.execute(CypherTestQueries.CountRelationships)

      result.hasNext should be (true)
      relCount = result.next().get("count").asInstanceOf[Long]

      tx.success()
    }finally {
      if(tx != null) tx.close()
    }

    nodeCount should be (15L)
    relCount should be (21L)

  }



}
