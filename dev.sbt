lazy val mainRunner = project.in(file("mainRunner")).dependsOn(RootProject(file("."))).settings(
  libraryDependencies ++= Seq(
    "org.apache.spark"  %  "spark-sql_2.11"     % "2.3.0" % "compile"
  )
)