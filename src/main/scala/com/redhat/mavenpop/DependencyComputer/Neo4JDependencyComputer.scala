package com.redhat.mavenpop.DependencyComputer
import org.apache.log4j.LogManager
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.neo4j.driver.v1.exceptions.ClientException
import org.neo4j.driver.v1.{AuthTokens, Config, GraphDatabase, Session}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import com.redhat.mavenpop.DependencyComputer.Neo4JDependencyComputer.TransactionFailureReason
import com.redhat.mavenpop.DependencyComputer.Neo4JDependencyComputer.TransactionFailureReason.TransactionFailureReason

object Neo4JDependencyComputer{

  object TransactionFailureReason extends Enumeration {
    type TransactionFailureReason = Value
    val TIMEOUT, OTHER = Value
  }
}

class Neo4JDependencyComputer(
                               boltUrl: String,
                               username: String,
                               password: String,
                               depth: Int,
                               withInstrumentation: Boolean,
                               testConfig: Boolean)
  extends DependencyComputer {

  def this(
            boltUrl: String,
            username: String,
            password: String,
            depth: Int,
            withInstrumentation: Boolean ) {

    this(boltUrl, username, password, depth, withInstrumentation, false)
  }

  def this(
            boltUrl: String,
            username: String,
            password: String,
            depth: Int ) {

    this(boltUrl, username, password, depth, false, false)
  }

  @transient private lazy val logger = LogManager.getLogger(getClass.getName)

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
    val query_ = CypherQueries.GetDependenciesFromListV2(depth)

    logger.info("starting to compute dependencies with query: " + query_)
    logger.info("database url: " + boltUrl_)

    // Use of map partitions to create one connection per partition in workers See:
    //  https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd

    // To test non concurrent calls to database:
    // val sessionsWithTimeRdd = sessions.rdd.repartition(1).mapPartitionsWithIndex {

    val sessionsWithTimeRdd = sessions.rdd.mapPartitionsWithIndex {
      case (partIndex, iter) =>

        //ToDo: instead of instantiating a new driver for each partition, consider a  connection pool

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

        val result = iter.zipWithIndex.map {
          case (row, index) =>

            //precondition: gavList.size >= 1
            logger.debug(s"started processing row $index")
            var resultRow: Row = row
            val gavList = row.getAs[Seq[String]]("gavs").asJava

            getDependencies(neo4jSession, query_, gavList, s"$partIndex-$index") match {
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
            logger.debug(s"finishing processing row $index")
            resultRow

        }.toList

        neo4jSession.close()
        driver.close()

        result.iterator
    }

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

  case class ProfilerResult(elapsedMillis: Long, dependencies: ArrayBuffer[String])

  /***
    * Gets the dependencies from database and return the dependencies or failure reason
    * @param neo4jSession
    * @param query
    * @param gavList
    * @param debugIndex
    * @return Either a result consisting of the dependencies obtained from the database
    *         and the elapsed time in milliseconds taked to get the result.
    *         if #withInstrumentation field is false, it does not calucalte the time returns -1 in the elapsed time field
    */
  private def getDependencies(neo4jSession: Session, query: String,
                              gavList: java.util.List[String], debugIndex: String):
  Either[TransactionFailureReason, ProfilerResult] = {

    //Using Either instead of Try/Success/Failure because the ClientError exception is not catched by scala.util.Try()
    val parameters = Map[String, Object]("gavList" -> gavList).asJava

    def getDependenciesFromDB(): ArrayBuffer[String] = {
      val _deps = new ArrayBuffer[String]()

      logger.debug(s"sending query (session.run) for row: $debugIndex")
      val result = neo4jSession.run(query, parameters)
      logger.debug(s"ended session.run call for row: $debugIndex")

      while (result.hasNext) {
        _deps.append(result.next().get(0).asString())
      }
      logger.debug(s"ended result consumption for row: $debugIndex")

      _deps
    }

    try {

      if(withInstrumentation) {
        val t1 = System.nanoTime()
        val dependencies = getDependenciesFromDB()
        val elapsedMillis = (System.nanoTime() - t1) / 1000000

        logger.debug(s"ROW: $debugIndex , ellapsedMillis: $elapsedMillis")

        return Right(ProfilerResult(elapsedMillis, dependencies))

      } else {
        val dependencies = getDependenciesFromDB()
        return Right(ProfilerResult(-1, dependencies))
      }

    } catch {
      case e: Throwable => {
        if (e.isInstanceOf[ClientException] && e.getMessage.contains("timeout")) {
          logger.debug(s"ROW $debugIndex Timed out")
          return Left(TransactionFailureReason.TIMEOUT)
        } else
          return Left(TransactionFailureReason.OTHER)
      }
    }
  }

  private def getTraversalWork(neo4jSession: Session, gavList: java.util.List[String]): Either[TransactionFailureReason, Int] = {

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
}
