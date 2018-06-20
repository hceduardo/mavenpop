
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
      "org.apache.spark"  %  "spark-sql_2.11"     % "2.3.0"  /*% "provided"*/,
      "org.neo4j.driver" %  "neo4j-java-driver"  % "1.5.2",
      "org.scalatest" %% "scalatest" % "3.0.1" % "test"
    )
  )