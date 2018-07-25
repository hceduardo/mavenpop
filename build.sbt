
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      scalaHome       := Some(file(sys.env.get("SCALA_HOME").getOrElse("/opt/scala"))),
      scalaVersion    := "2.11.8",
      organization    := "com.redhat.mavenpop"
    )),
    name := "mavenpopcore",
    version := "0.1",
    mainClass in assembly := Some("com.redhat.mavenpop.DependencyParser.DependencyParserApp"),
    libraryDependencies ++= Seq(

      "log4j" % "log4j" % "1.2.17" % Compile,
      "org.apache.spark"  %  "spark-sql_2.11"     % "2.3.0" % Provided,
      "org.neo4j.driver" %  "neo4j-java-driver"  % "1.5.2",
      "com.typesafe" % "config"               % "1.3.3",

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
    ),
    parallelExecution in Test := false
  )