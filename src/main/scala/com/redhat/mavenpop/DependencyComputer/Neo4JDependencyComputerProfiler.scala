package com.redhat.mavenpop.DependencyComputer

import com.redhat.mavenpop.TransactionFailureReason
import com.redhat.mavenpop.TransactionFailureReason.TransactionFailureReason
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.neo4j.driver.v1.exceptions.ClientException
import org.neo4j.driver.v1.{ AuthTokens, Config, GraphDatabase, Session }

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import Neo4JDependencyComputerProfiler._
import org.apache.log4j.LogManager

object Neo4JDependencyComputerProfiler {

  @transient private lazy val logger = LogManager.getLogger(getClass.getName)
  val defaultDepth: Short = 1000

  case class ProfilerResult(elapsedMillis: Long, dependencies: ArrayBuffer[String])

  def getTraversalWork(neo4jSession: Session, gavList: java.util.List[String]): Either[TransactionFailureReason, Int] = {

    //Using Either instead of Try/Success/Failure because the ClientError exception is not catched by scala.util.Try()

    val parameters = Map[String, Object]("gavList" -> gavList).asJava

    try {

      //      val t1 = System.nanoTime()

      val result = neo4jSession.run(CypherQueries.GetTraversalWork, parameters)

      val traversalWork = result.single().get(0).asInt()

      //      val elapsedMillis = (System.nanoTime() - t1) / 1000000

      Right(traversalWork)

    } catch {
      case e: Throwable => {
        if (e.isInstanceOf[ClientException] && e.getMessage.contains("timeout"))
          Left(TransactionFailureReason.TIMEOUT)
        else
          Left(TransactionFailureReason.OTHER)
      }
    }
  }

  def getDependencies(neo4jSession: Session, gavList: java.util.List[String], depth: Int): Either[TransactionFailureReason, ProfilerResult] = {

    //Using Either instead of Try/Success/Failure because the ClientError exception is not catched by scala.util.Try()

    val parameters = Map[String, Object]("gavList" -> gavList).asJava

    try {

      val t1 = System.nanoTime()

      val result = neo4jSession.run(CypherQueries.GetDependenciesFromList(depth), parameters)
      val deps = new ArrayBuffer[String]()

      while (result.hasNext) {
        deps.append(result.next().get(0).asString())
      }

      val elapsedMillis = (System.nanoTime() - t1) / 1000000

      logger.debug(s"Query: ${CypherQueries.GetDependenciesFromList(depth)}, gavs: ${gavList.asScala.mkString(",")}")

      Right(ProfilerResult(elapsedMillis, deps))

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

//@SerialVersionUID(100L)
class Neo4JDependencyComputerProfiler(
  boltUrl: String, username: String, password: String,
  depth: Int, testConfig: Boolean)
  extends DependencyComputer {

  @transient private lazy val logger = LogManager.getLogger(getClass.getName)

  def this(boltUrl: String, username: String, password: String, depth: Int) {
    this(boltUrl, username, password, depth, false)
  }

  def this(boltUrl: String, username: String, password: String) {
    this(boltUrl, username, password, defaultDepth, false)
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

    // Copying instance fields to local variables to avoid serializing instance for executor task subimission
    // ref: https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark

    val (boltUrl_, username_, password_, testConfig_) = (boltUrl, username, password, testConfig)

    // Use of map partitions to create one connection per partition in workers See:
    //  https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd

    val sessionsWithTimeRdd = sessions.rdd.mapPartitions(iter => {

      //ToDo: instead of instantiating a new driver for each partition, consider a  connection pool
      //      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password))

      val driver = if (testConfig_)
        GraphDatabase.driver(boltUrl_, AuthTokens.basic(username_, password_), Config.build().
          withoutEncryption().
          toConfig)
      else
        GraphDatabase.driver(boltUrl_, AuthTokens.basic(username_, password_), Config.defaultConfig())

      val neo4jSession = driver.session

      //using toList to force eager computation of the map. Otherwise the connection is closed before the map is computed
      //ToDo: evaluate better ways of forcing eager computation (for ex: closing the connection inside the map if(iter.isempty))
      // https://stackoverflow.com/questions/36545579/spark-how-to-use-mappartition-and-create-close-connection-per-partition/36545821#36545821
      val result = iter.map(row => {

        //precondition: gavList.size >= 1

        var resultRow: Row = row
        val gavList = row.getAs[Seq[String]]("gavs").asJava

        getDependencies(neo4jSession, gavList, depth) match {
          case Right(result) => {
            // add dependencies, elapsed computing in milliseconds and errorDeps = null
            resultRow = Row.fromSeq(resultRow.toSeq ++ Array(result.dependencies, result.elapsedMillis, null))
          }

          case Left(failureReason) => {
            // add dependencies = null, elapsed time = None with failure reason
            resultRow = Row.fromSeq(resultRow.toSeq ++ Array(null, null, failureReason.toString))
          }
        }

        //        getTraversalWork(neo4jSession, gavList) match {
        //          case Right(traversalWork: Int) => {
        //            // add traversalwork and errorTrav = null
        //            resultRow = Row.fromSeq(resultRow.toSeq ++ Array(traversalWork, null))
        //          }
        //          case Left(failureReason) => {
        //            // add dependencies = null,  with failure reason
        //            resultRow = Row.fromSeq(resultRow.toSeq ++ Array(null, failureReason.toString))
        //          }
        //        }

        resultRow

      }).toList

      neo4jSession.close()
      driver.close()

      result.iterator
    })

    val newSchema = StructType(sessions.schema.fields ++ Array[StructField](
      StructField("dependencies", ArrayType(StringType, true), true), StructField("execMillis", LongType, true), StructField("errorDeps", StringType, true)
    //      , StructField("traversalWork", IntegerType, true)
    //      , StructField("errorTrav", StringType, true)
    ))

    val sessionsWithTime = spark.createDataFrame(sessionsWithTimeRdd, newSchema)
    //    sessionsWithTime.cache

    sessionsWithTime
    // Testing
    /*
        sessionsWithTime.
          select($"gavs", $"topLevelGavs").
          filter( size($"gavs") =!= size($"topLevelGavs") ).
          show(10, 0, true)

        sessionsWithTime.
          select($"gavs", $"topLevelGavs").
          filter( (size($"gavs") === size($"topLevelGavs"))).
          show(10, 0, true)

        /// write for testing
        import org.apache.spark.sql.functions.udf
        val stringify = udf((vs: Seq[String]) => s"""[${vs.mkString(",")}]""")

        sessionsWithTime.
          select($"gavs", $"topLevelGavs").
          filter( size($"gavs") =!= size($"topLevelGavs") ).
          withColumn("gavs", stringify($"gavs")).withColumn("topLevelGavs", stringify($"topLevelGavs")).
          write.csv("/home/edhdz/l/mavenpop/data/tmp/analytics_graph_results.csv")
    */
  }

}
