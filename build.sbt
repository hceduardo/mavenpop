
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      scalaHome       := Some(file("/opt/scala")),
      scalaVersion    := "2.11.8",
      organization    := "com.redhat.mavenpop"
    )),
    name := "mavenpopcore",
    version := "0.1",
    mainClass in assembly := Some("com.redhat.mavenpop.MavenPop"),
    libraryDependencies ++= Seq(
      "org.apache.spark"  %  "spark-sql_2.11"     % "2.3.0" /*% "provided"*/,
      "org.neo4j.driver" %  "neo4j-java-driver"  % "1.5.2",

      "org.scalatest" %% "scalatest" % "3.0.1" % Test,
      "org.apache.spark"  %  "spark-core_2.11"     % "2.3.0" % Test classifier "tests",
      "org.apache.spark"  %  "spark-catalyst_2.11"     % "2.3.0" % Test classifier "tests",
      "org.apache.spark"  %  "spark-sql_2.11"     % "2.3.0"  % Test classifier "tests",
      "org.neo4j" % "neo4j" % "3.4.0" % Test,
      "org.neo4j" % "neo4j-kernel" % "3.4.0" % Test classifier "tests",
      "org.neo4j" % "neo4j-io" % "3.4.0" % Test classifier "tests",
      "org.neo4j" % "neo4j-bolt" % "3.4.0" % Test,
      "junit" % "junit" % "4.12" % Test,
      "org.hamcrest" % "hamcrest-all" % "1.3" % Test
    )
  )