package com.redhat.mavenpop

import java.util

import com.redhat.mavenpop.DependencyComputer.CypherQueries
import com.redhat.mavenpop.TransactionFailureReason.TransactionFailureReason
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.exceptions.ClientException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object Experimentv2 {

  def main(args: Array[String]): Unit = {
    // Load resources/reference.conf by default
    // Allows override with -Dconfig.file=path/to/config-file
    val conf: MavenPopConfig = new MavenPopConfig()

    val driver = GraphDatabase.driver(
      conf.neoBoltUrl,
      AuthTokens.basic(conf.neoUsername, conf.neoPassword), Config.defaultConfig())
    val session: Session = driver.session()

    val gavList21 = "junit:junit:3.8.1|org.apache.maven:maven-artifact-manager:2.0.6|org.apache.maven:maven-parent:4|org.codehaus.plexus:plexus-utils:1.0.4|org.apache.maven.shared:maven-shared-io:1.1|org.apache.maven.shared:maven-shared-components:8|org.apache.maven:maven-profile:2.0.6|org.apache.maven:maven-repository-metadata:2.0.6|org.apache.maven:maven-plugin-registry:2.0.6|org.codehaus.plexus:plexus:1.0.4|classworlds:classworlds:1.1-alpha-2|org.codehaus.plexus:plexus:1.0.11|org.codehaus.plexus:plexus-utils:1.4.1|org.apache.maven:maven-artifact:2.0.6|org.codehaus.plexus:plexus-container-default:1.0-alpha-9-stable-1|org.apache.maven:maven-settings:2.0.6|org.apache.maven.wagon:wagon-provider-api:1.0-beta-2|org.apache.maven:maven-parent:7|org.apache.maven:maven-model:2.0.6|org.codehaus.plexus:plexus-containers:1.0.3|org.apache.maven.wagon:wagon:1.0-beta-2".
      split('|')

    //:param gavList => split("junit:junit:3.8.1|org.apache.maven:maven-artifact-manager:2.0.6|org.apache.maven:maven-parent:4|org.codehaus.plexus:plexus-utils:1.0.4|org.apache.maven.shared:maven-shared-io:1.1|org.apache.maven.shared:maven-shared-components:8|org.apache.maven:maven-profile:2.0.6|org.apache.maven:maven-repository-metadata:2.0.6|org.apache.maven:maven-plugin-registry:2.0.6|org.codehaus.plexus:plexus:1.0.4|classworlds:classworlds:1.1-alpha-2|org.codehaus.plexus:plexus:1.0.11|org.codehaus.plexus:plexus-utils:1.4.1|org.apache.maven:maven-artifact:2.0.6|org.codehaus.plexus:plexus-container-default:1.0-alpha-9-stable-1|org.apache.maven:maven-settings:2.0.6|org.apache.maven.wagon:wagon-provider-api:1.0-beta-2|org.apache.maven:maven-parent:7|org.apache.maven:maven-model:2.0.6|org.codehaus.plexus:plexus-containers:1.0.3|org.apache.maven.wagon:wagon:1.0-beta-2","|")
    val gavList10 = "org.eclipse.birt.runtime:org.eclipse.core.resources:3.9.1.v20140825-1431|org.eclipse.birt.runtime:org.eclipse.emf.ecore.change:2.10.0.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.emf:2.6.0.v20150123-0452|org.eclipse.birt.runtime:org.eclipse.datatools.enablement.msft.sqlserver:1.0.2.v201212120617|org.eclipse.birt.runtime:org.eclipse.osgi:3.10.2.v20150203-1939|org.eclipse.birt.runtime:org.eclipse.emf.ecore.xmi:2.10.2.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.emf.common:2.10.1.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.core.jobs:3.6.1.v20141014-1248|org.eclipse.birt.runtime:org.eclipse.emf.ecore:2.10.2.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.birt.runtime:4.4.2".
      split('|')

    // :param gavList => split("org.eclipse.birt.runtime:org.eclipse.core.resources:3.9.1.v20140825-1431|org.eclipse.birt.runtime:org.eclipse.emf.ecore.change:2.10.0.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.emf:2.6.0.v20150123-0452|org.eclipse.birt.runtime:org.eclipse.datatools.enablement.msft.sqlserver:1.0.2.v201212120617|org.eclipse.birt.runtime:org.eclipse.osgi:3.10.2.v20150203-1939|org.eclipse.birt.runtime:org.eclipse.emf.ecore.xmi:2.10.2.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.emf.common:2.10.1.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.core.jobs:3.6.1.v20141014-1248|org.eclipse.birt.runtime:org.eclipse.emf.ecore:2.10.2.v20150123-0348|org.eclipse.birt.runtime:org.eclipse.birt.runtime:4.4.2", "|")
    val gavList5 = "org.apache.maven.plugins:maven-install-plugin|org.apache.maven.plugins:maven-deploy-plugin|org.codehaus:mojo|org.codehaus.mojo:cobertura-maven-plugin|org.apache.maven:plugins".
      split('|')
    // :param gavList => split("org.apache.maven.plugins:maven-install-plugin|org.apache.maven.plugins:maven-deploy-plugin|org.codehaus:mojo|org.codehaus.mojo:cobertura-maven-plugin|org.apache.maven:plugins", "|")

    print("gavList.len -> elapsedMillis\n")
    for (gavList <- Array(gavList5, gavList10, gavList21)) {
      getDependencies(session, gavList) match {
        case Right(result: Result) => print(s"${gavList.length} -> ${result.elapsedMillis}")
        case Left(failReason) => print(s"${gavList.length} -> $failReason")
      }
      print("\n")
    }

    session.close()
    driver.close()

  }

  case class Result(elapsedMillis: Long, dependencies: ArrayBuffer[String])

  private def getDependencies(session: Session, gavList: Array[String]): Either[TransactionFailureReason, Result] = {

    //Using Either instead of Try/Success/Failure because the ClientError exception is not catched by scala.util.Try()

    val parameters = Map[String, Object]("gavList" -> gavList).asJava

    try {

      val t1 = System.nanoTime()

      val result = session.run(CypherQueries.GetDependenciesFromList, parameters)
      val deps = new ArrayBuffer[String]()

      while (result.hasNext) {
        deps.append(result.next().get(0).asString())
      }

      val elapsedMillis = (System.nanoTime() - t1) / 1000000

      Right(Result(elapsedMillis, deps))

    } catch {
      case e: Throwable => {
        if (e.isInstanceOf[ClientException] && e.getMessage.contains("timeout"))
          Left(TransactionFailureReason.TIMEOUT)
        else
          Left(TransactionFailureReason.OTHER)
      }
    }
  }

}