package com.redhat.mavenpop

import com.redhat.mavenpop.DependencyComputer.CypherQueries
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.exceptions.ClientException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.redhat.mavenpop.TransactionFailureReason.TransactionFailureReason

object Experiment {

  def main(args: Array[String]): Unit = {
    // Load resources/reference.conf by default
    // Allows override with -Dconfig.file=path/to/config-file
    val conf: MavenPopConfig = new MavenPopConfig()

    val driver = GraphDatabase.driver(
      conf.neoBoltUrl,
      AuthTokens.basic(conf.neoUsername, conf.neoPassword), Config.defaultConfig())
    val session: Session = driver.session()

    val in21 = "junit:junit:3.8.1|org.apache.maven:maven-artifact-manager:2.0.6|org.apache.maven:maven-parent:4|org.codehaus.plexus:plexus-utils:1.0.4|org.apache.maven.shared:maven-shared-io:1.1|org.apache.maven.shared:maven-shared-components:8|org.apache.maven:maven-profile:2.0.6|org.apache.maven:maven-repository-metadata:2.0.6|org.apache.maven:maven-plugin-registry:2.0.6|org.codehaus.plexus:plexus:1.0.4|classworlds:classworlds:1.1-alpha-2|org.codehaus.plexus:plexus:1.0.11|org.codehaus.plexus:plexus-utils:1.4.1|org.apache.maven:maven-artifact:2.0.6|org.codehaus.plexus:plexus-container-default:1.0-alpha-9-stable-1|org.apache.maven:maven-settings:2.0.6|org.apache.maven.wagon:wagon-provider-api:1.0-beta-2|org.apache.maven:maven-parent:7|org.apache.maven:maven-model:2.0.6|org.codehaus.plexus:plexus-containers:1.0.3|org.apache.maven.wagon:wagon:1.0-beta-2"

    val in10 = "org.eclipse.birt.runtime:org.eclipse.core.resources:3.9.1.v20140825-1431|org.eclipse.birt.runtime:org.eclipse.emf.ecore.change:2.10.0.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.emf:2.6.0.v20150123-0452|org.eclipse.birt.runtime:org.eclipse.datatools.enablement.msft.sqlserver:1.0.2.v201212120617|org.eclipse.birt.runtime:org.eclipse.osgi:3.10.2.v20150203-1939|org.eclipse.birt.runtime:org.eclipse.emf.ecore.xmi:2.10.2.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.emf.common:2.10.1.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.core.jobs:3.6.1.v20141014-1248|org.eclipse.birt.runtime:org.eclipse.emf.ecore:2.10.2.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.birt.runtime:4.4.2"

    val in5 = "org.apache.maven.plugins:maven-install-plugin|org.apache.maven.plugins:maven-deploy-plugin|org.codehaus:mojo|org.codehaus.mojo:cobertura-maven-plugin|org.apache.maven:plugins"

    val gavList = in21.split("|")
    val parameters = Map[String, Object]("gavList" -> gavList).asJava

    //    val queryResult: Either[String, StatementResult] =

    //    session.run ("call dbms.setConfigValue('dbms.transaction.timeout', '2s')")

    try {

      session.readTransaction(new TransactionWork[ArrayBuffer[String]] {
        override def execute(tx: Transaction): ArrayBuffer[String] = {

          //          print(CypherQueries.GetDependenciesFromList)
          //          print(parameters.get("gavList").asInstanceOf[Array[String]])

          val result = tx.run(CypherQueries.GetDependenciesFromList, parameters)
          val deps = new ArrayBuffer[String]()

          while (result.hasNext) {
            deps.append(result.next().get(0).asString())
          }

          deps
        }
      })

    } catch {
      case e: Throwable => print("haaaaas" + e.printStackTrace()) //Left(e.getMessage)
    }

    //    queryResult match {
    //      case Right(result) => print("res")
    //      case Left(fail) => print(fail)
    //    }

    //    val dependencies: Either[String, ArrayBuffer[String]] = try {
    //      print("trying")
    //
    //      val queryResult =
    //
    //      val deps = new ArrayBuffer[String]
    //
    //      while (queryResult.hasNext()){
    //        deps.append(queryResult.next().get("dependencyId").asString())
    //      }
    //
    //      Right(deps)
    //
    //    } catch {
    //
    //      case e:Throwable  => Left(e.getMessage)
    //    }
    //
    //    dependencies match {
    //      case Left(reason) => print(reason)
    //      case Right(deps) => print(deps.length)
    //
    //    }

    session.close()
    session.close()
    driver.close()
  }
}
