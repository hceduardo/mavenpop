package com.redhat.mavenpop.UsageAnalyser
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.neo4j.driver.v1.{AuthTokens, Config, GraphDatabase}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object Neo4jSessionAnalyser{
  object CypherQueries {

    /***
      * Returns which of the gavs in the list are dependencies of other gavs in the list
      * params: gavList
      * return columns: dependencyId
      */
    val GetDependenciesFromList :String =
      """WITH $gavList as gavIds
MATCH p = (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV)
WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
ANY (gavId in gavIds WHERE (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
RETURN DISTINCT dependency.id AS dependencyId"""

  }
}

@SerialVersionUID(100L)
class Neo4jSessionAnalyser(boltUrl: String,
                           username: String,
                           password: String,
                           testConfig: Boolean )
  extends SessionAnalyser {

  def this (boltUrl: String,
            username: String,
            password: String){
    this(boltUrl,username,password, false)
  }

  /***
    * Read gavs list of each session and computes which of them are dependencies of other gavs in the list
    * Adds the dependencies list as a new "dependencies" column and returns that new dataframe
    *
    * Uses Neo4j to compute the dependencies
    *
    * @param spark spark session
    * @param sessions Dataframe with sessions:
         StructType(List(
           StructField("clientId",IntegerType,false),
           StructField("sessionId",LongType,true),
           StructField("startTime",LongType,true),
           StructField("endTime",LongType,true),
           StructField("gavs",ArrayType(StringType,true),true))
    * @return Dataframe with dependencies column added
         StructType(List(
           StructField("clientId",IntegerType,false),
           StructField("sessionId",LongType,true),
           StructField("startTime",LongType,true),
           StructField("endTime",LongType,true),
           StructField("gavs",ArrayType(StringType,true),true),
           StructField("dependencies", ArrayType(StringType, true), true))
    *
    */
  override def computeDependencies(spark: SparkSession, sessions: DataFrame): DataFrame = {

    // Use of map partitions to create one connection per partition in workers See:
    //  https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd

    val sessionsWithDependenciesRdd = sessions.rdd.mapPartitions(iter => {

      //ToDo: instead of instantiating a new driver for each partition, consider a  connection pool
//      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password))

      val driver = if (testConfig)
        GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password), Config.build().withoutEncryption().toConfig)
      else
        GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password))

      val session = driver.session

      //using toList to force eager computation of the map. Otherwise the connection is closed before the map is computed
      //ToDo: evaluate better ways of forcing eager computation (for ex: closing the connection inside the map if(iter.isempty))
      // https://stackoverflow.com/questions/36545579/spark-how-to-use-mappartition-and-create-close-connection-per-partition/36545821#36545821
      val result = iter.map(row => {

        //precondition: gavList.size >= 1

        val gavList = row.getAs[Seq[String]]("gavs").asJava
        val dependencies = new ArrayBuffer[String]()

        if (gavList.size > 1) {
          val parameters = Map[String, Object]("gavList" -> gavList).asJava
          val queryResult = session.run(CypherQueries.GetDependenciesFromList, parameters)

          while (queryResult.hasNext()) {
            dependencies.append(queryResult.next().get("dependencyId").asString())
          }
        }

        Row.fromSeq(row.toSeq :+ dependencies) // add array as "column" to rdd

      }).toList

      session.close()
      driver.close()

      result.iterator
    })

    val newSchema = StructType(sessions.schema.fields :+ StructField("dependencies", ArrayType(StringType, true), true))
    val sessionsWithDependencies = spark.createDataFrame(sessionsWithDependenciesRdd, newSchema)
    //    sessionsWithDependencies.cache


    sessionsWithDependencies
    // Testing
    /*
        sessionsWithDependencies.
          select($"gavs", $"topLevelGavs").
          filter( size($"gavs") =!= size($"topLevelGavs") ).
          show(10, 0, true)

        sessionsWithDependencies.
          select($"gavs", $"topLevelGavs").
          filter( (size($"gavs") === size($"topLevelGavs"))).
          show(10, 0, true)

        /// write for testing
        import org.apache.spark.sql.functions.udf
        val stringify = udf((vs: Seq[String]) => s"""[${vs.mkString(",")}]""")

        sessionsWithDependencies.
          select($"gavs", $"topLevelGavs").
          filter( size($"gavs") =!= size($"topLevelGavs") ).
          withColumn("gavs", stringify($"gavs")).withColumn("topLevelGavs", stringify($"topLevelGavs")).
          write.csv("/home/edhdz/l/mavenpop/data/tmp/analytics_graph_results.csv")
    */
  }
}
