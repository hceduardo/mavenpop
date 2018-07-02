package com.redhat.mavenpop.DependencyComputer

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DependencyComputer extends Serializable{
  //Todo: pass structure with unknown and nodeps to filter sessions before computing

  /***
    * Read gavs list of each session and computes which of them are dependencies of other gavs in the list
    * Adds the dependencies list as a new "dependencies" column and returns that new dataframe
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
  def computeDependencies(spark: SparkSession, sessions: DataFrame): DataFrame
}
