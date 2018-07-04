package com.redhat.mavenpop

import com.redhat.mavenpop.DependencyComputer.CypherQueries
import org.neo4j.driver.v1.{ AuthTokens, Config, GraphDatabase, Session }
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object Experiment {

  private val Neo4jboltUrl = "bolt://localhost:7687"
  private val Neo4jUsername = "neo4j"
  private val Neo4jPassword = "Neo03"

  def main(args: Array[String]): Unit = {
    val driver = GraphDatabase.driver(Neo4jboltUrl, AuthTokens.basic(Neo4jUsername, Neo4jPassword), Config.defaultConfig())
    val session: Session = driver.session()

    val in1 = "junit:junit:3.8.1|org.apache.maven:maven-artifact-manager:2.0.6|org.apache.maven:maven-parent:4|org.codehaus.plexus:plexus-utils:1.0.4|org.\napache.maven.shared:maven-shared-io:1.1|org.apache.maven.shared:maven-shared-components:8|org.apache.maven:maven-profile:2.0.6|org.apache.m\naven:maven-repository-metadata:2.0.6|org.apache.maven:maven-plugin-registry:2.0.6|org.codehaus.plexus:plexus:1.0.4|classworlds:classworlds:\n1.1-alpha-2|org.codehaus.plexus:plexus:1.0.11|org.codehaus.plexus:plexus-utils:1.4.1|org.apache.maven:maven-artifact:2.0.6|org.codehaus.ple\nxus:plexus-container-default:1.0-alpha-9-stable-1|org.apache.maven:maven-settings:2.0.6|org.apache.maven.wagon:wagon-provider-api:1.0-beta-\n2|org.apache.maven:maven-parent:7|org.apache.maven:maven-model:2.0.6|org.codehaus.plexus:plexus-containers:1.0.3|org.apache.maven.wagon:wag\n\non:1.0-beta-2"

    val gavList = in1.split("|")
    val dependencies = new ArrayBuffer[String]()

    val parameters = Map[String, Object]("gavList" -> gavList).asJava
    val queryResult = session.run(CypherQueries.GetDependenciesFromList, parameters)

    while (queryResult.hasNext()) {
      dependencies.append(queryResult.next().get("dependencyId").asString())
    }

    print(dependencies)

    session.close()
    driver.close()
  }
}
