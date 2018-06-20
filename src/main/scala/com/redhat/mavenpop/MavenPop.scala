package com.redhat.mavenpop

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 */

// Model

case class RepoLogEntry(
  clientId: Integer,
  timestamp: java.sql.Timestamp, agent: String, path: String)

case class GavEntry(path: String, gav: String, dep_gavs: Array[String])

object MavenPop {

  private val RepologPath = "/home/edhdz/l/mavenpop/data_sample/repolog-part0000.txt"

  private val GavsDepsPath = "/home/edhdz/l/mavenpop/data/distinct_paths_inferred_gavs_with_deps.txt"

  private val ReportOutDir = "/home/edhdz/l/projects/mavenpopcore/out/reports"

  private val Neo4jURL = "bolt://localhost:7687"

  private val Neo4jUsername = "mavenpop"

  private val Neo4jPassword = "mavenpop"
  //
  //  private val Neo4jURL = "bolt://localhost:17687"
  //
  //  private val Neo4jUsername = "neo4j"
  //
  //  private val Neo4jPassword = "neo4j"

  private val MaxSessionIdleTime = 15 * 60

  private val ReportDate = "2015-05-13" // date with more distinct clients for part-00000: 2015-05-13, see 4_checkDates.scala

  private val SparkMasterURL = "local[*]"

  def main(args: Array[String]) {

    //todo: validate dataframe schema inside functions

    val sparkMaster = if (args.isEmpty) SparkMasterURL else args(0)

    val spark = SparkSession
      .builder
      .appName("MavenPop")
      .config("spark.master", sparkMaster)
      .config("spark.eventLog.enabled", true)
      .getOrCreate()

    import spark.implicits._

    val repoGavDF = loadGavsAndDependencies(spark, ReportDate, RepologPath, GavsDepsPath)

    val sessionOrg = groupGavsBySessions(spark, repoGavDF, MaxSessionIdleTime)

    //sessionOrg.write.mode(SaveMode.Overwrite).parquet(s"/home/edhdz/l/mavenpop/data/tmp/sessions${ReportDate}.parquet")
    //val sessionOrg = spark.read.parquet(s"/home/edhdz/l/mavenpop/data/tmp/sessions${ReportDate}.parquet")

    //REMOVE ME after optimizing query
    val sessionDF: Dataset[Row] = filterClientsWithShortSessions(spark, sessionOrg)
    sessionDF.cache()

    val sessTopLevelDF = computeTopLevelDependencies(spark, sessionDF, Neo4jURL, Neo4jUsername, Neo4jPassword)

    //sessTopLevelDF.write.mode(SaveMode.Overwrite).parquet(s"/home/edhdz/l/mavenpop/data/tmp/sess_top_level_${ReportDate}.parquet")
    //val sessTopLevelDF = spark.read.parquet(s"/home/edhdz/l/mavenpop/data/tmp/sess_top_level_${ReportDate}.parquet")

    writeReport(spark, sessTopLevelDF, ReportOutDir)

    spark.stop()

  }

  //Todo:after optimizing neo remove this
  private def filterClientsWithShortSessions(spark: SparkSession, sessionOrg: DataFrame) = {

    import spark.implicits._

    val clientsWithShortSessions =
      sessionOrg.groupBy("clientId").agg(max(size($"gavs")).as("maxgavs")).
        filter($"maxgavs" <= 10).select("clientId").
        collect.map(_.getInt(0)).toList

    val sessionDF = sessionOrg.filter($"clientId".isin(clientsWithShortSessions: _*))

    sessionDF
  }

  private def loadGavsAndDependencies(spark: SparkSession, reportDate: String, repologPath: String, gavsDepsPath: String) = {
    import java.sql.Timestamp
    import spark.implicits._

    //todo exception if file does not exists
    ///// Data import

    println(s"importing data for date ${reportDate}")

    val repologDF = spark.read.textFile(repologPath).
      map(_.split(" ")).
      map(attrs => RepoLogEntry(
        attrs(0).trim.toInt,
        new Timestamp(attrs(1).trim.toLong), attrs(2).trim, attrs(3).trim)).
      filter(to_date($"timestamp") === reportDate)

    val gavDF = spark.read.textFile(gavsDepsPath).
      map(_.split(" ")).
      map(attrs => GavEntry(attrs(1).trim, attrs(2).trim, attrs(3).trim.split(",")))

    //ToDo: throw exception here
    // Check uniqueness of path (assert true)
    gavDF.select("path").distinct.count == gavDF.count

    // Join gavs and dependencies

    val repogavDF = repologDF.as("r").join(gavDF.as("g"), $"r.path" === $"g.path").
      select("clientId", "timestamp", "r.path", "gav", "dep_gavs").
      orderBy(asc("clientId"), asc("timestamp"))

    repogavDF
  }

  private def groupGavsBySessions(spark: SparkSession, repoGavDF: DataFrame, maxSessionIdleTime: Int) = {

    import spark.implicits._
    import org.apache.spark.sql.expressions.Window

    ////// Sessionization

    // Good reference: http://www.janvsmachine.net/2017/09/sessionization-with-spark.html

    //ToDo: [tuning] make sure elements of same partition are in the same worker
    //ensure repogav is ordered in timestamp asc

    /// Window prev timestamp

    val partitionWindow = Window.partitionBy("clientId").orderBy("clientId", "timestamp")

    println(s"Sessionizating with max idle time ${maxSessionIdleTime} seconds")

    val repoGavWithSessIdDF = repoGavDF.
      withColumn("prev", lag("timestamp", 1).over(partitionWindow)).
      withColumn("diff", $"timestamp".cast("Long") - $"prev".cast("Long")).
      withColumn(
        "isNewSession",
        when($"diff" > lit(maxSessionIdleTime), lit(1)).otherwise(lit(0))).
        withColumn(
          "sessionId",
          sum($"isNewSession").over(partitionWindow)).
          select("clientId", "timestamp", "path", "gav", "sessionId")

    //repoGavWithSessIdDF.show(50)

    val sessionDF = repoGavWithSessIdDF.groupBy("clientId", "sessionId").
      agg(
        min("timestamp").as("startTime"),
        max("timestamp").as("endTime"),
        collect_set("gav").as("gavs"))

    println("Sessionization finished")
    sessionDF
  }

  private def computeTopLevelDependencies(spark: SparkSession, sessionDF: DataFrame, neo4jURL: String, username: String, password: String) = {
    import org.neo4j.driver.v1._
    import spark.implicits._
    import scala.collection.mutable.ArrayBuffer

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{ IntegerType, StructField, StructType, ArrayType, StringType }
    import scala.collection.JavaConverters._

    println("computing dependencies")

    //Todo: make the alias of the return a parameter, needs to be the same in result.next().getAs
    // [*0..] in case there are gavs included in dependency graph that are not related to any other in gavList, or all others are not in graph
    val cypherQuery =
      """WITH $gavList as gavIds
  MATCH p = (topLevel)-[*0..]->(dependency)
  WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
    NONE (gavId in gavIds WHERE (:GAV{id:gavId})-[:DEPENDS_ON]->(topLevel))
  RETURN DISTINCT topLevel.id AS TopLevelGavID
  UNION
  UNWIND $gavList as gavId
  OPTIONAL MATCH (gav {id: gavId})
  WITH gav, gavId
  WHERE gav IS NULL
  RETURN gavId AS TopLevelGavID"""

    // Use of map partitions to create one connection per partition in workers See:
    //  https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd

    val sessTopLevelRDD = sessionDF.rdd.mapPartitions(iter => {

      //ToDo: instead of instantiating a new driver for each partition, consider a  connection pool
      val driver = GraphDatabase.driver(neo4jURL, AuthTokens.basic(username, password))
      val session = driver.session

      //using toList to force eager computation of the map. Otherwise the connection is closed before the map is computed
      //ToDo: evaluate better ways of forcing eager computation (for ex: closing the connection inside the map if(iter.isempty))
      // https://stackoverflow.com/questions/36545579/spark-how-to-use-mappartition-and-create-close-connection-per-partition/36545821#36545821
      val result = iter.map(row => {

        //precondition: gavList.size >= 1

        val gavList = row.getAs[Seq[String]]("gavs").asJava
        var topLevelGavs = new ArrayBuffer[String]()

        if (gavList.size == 1) {
          topLevelGavs.append(gavList.get(0))
        } else {
          val parameters = Map[String, Object]("gavList" -> gavList).asJava
          val queryResult = session.run(cypherQuery, parameters)

          while (queryResult.hasNext()) {
            topLevelGavs.append(queryResult.next().get("TopLevelGavID").asString())
          }
        }

        Row.fromSeq(row.toSeq :+ topLevelGavs) // add array as "column" to rdd

      }).toList

      session.close()
      driver.close()

      result.iterator
    })

    val newSchema = StructType(sessionDF.schema.fields :+ StructField("topLevelGavs", ArrayType(StringType, true), true))
    val sessTopLevelDF = spark.createDataFrame(sessTopLevelRDD, newSchema)
    //    sessTopLevelDF.cache

    println("computing dependencies finished")
    sessTopLevelDF
    // Testing
    /*
        sessTopLevelDF.
          select($"gavs", $"topLevelGavs").
          filter( size($"gavs") =!= size($"topLevelGavs") ).
          show(10, 0, true)

        sessTopLevelDF.
          select($"gavs", $"topLevelGavs").
          filter( (size($"gavs") === size($"topLevelGavs"))).
          show(10, 0, true)

        /// write for testing
        import org.apache.spark.sql.functions.udf
        val stringify = udf((vs: Seq[String]) => s"""[${vs.mkString(",")}]""")

        sessTopLevelDF.
          select($"gavs", $"topLevelGavs").
          filter( size($"gavs") =!= size($"topLevelGavs") ).
          withColumn("gavs", stringify($"gavs")).withColumn("topLevelGavs", stringify($"topLevelGavs")).
          write.csv("/home/edhdz/l/mavenpop/data/tmp/analytics_graph_results.csv")
    */
  }

  private def writeReport(spark: SparkSession, sessTopLevelDF: DataFrame, outDir: String) = {
    import spark.implicits._
    import scala.collection.mutable.ArrayBuffer
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.functions.udf

    val diff = udf((gavs: Seq[String], topLevel: Seq[String]) => gavs.diff(topLevel))

    //val sessTopLevelDF = spark.read.parquet("/home/edhdz/l/mavenpop/data/tmp/sess_top_level_79.parquet")

    println("counting usage")

    val directIndirect = sessTopLevelDF.withColumn("indirectSeq", diff($"gavs", $"topLevelGavs")).
      withColumnRenamed("topLevelGavs", "directSeq")

    directIndirect.cache()

    // Testing: gavs.size = indirect.size + direct.size
    // directIndirect.select(size($"gavs"), size($"indirectSeq"), size ($"directSeq") ).show

    val directDF = directIndirect.select("clientId", "startTime", "endTime", "directSeq")
    val indirectDF = directIndirect.select("clientId", "startTime", "endTime", "indirectSeq")

    // probably use window functions here to report dates. use it right after explode

    val outPath = outDir + "/" + ReportDate

    directDF.withColumn("direct", explode($"directSeq")).
      groupBy("direct").agg(countDistinct("clientId").as("clients")).
      write.mode(SaveMode.Overwrite).csv(outPath + "_direct.csv")

    indirectDF.withColumn("indirect", explode($"indirectSeq")).
      groupBy("indirect").agg(countDistinct("clientId").as("clients")).
      write.mode(SaveMode.Overwrite).csv(outPath + "_indirect.csv")

    println(s"writing output to ${outPath}")
  }

}
