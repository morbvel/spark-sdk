package org.gft.scalasparksdk.core

import org.apache.spark.sql.SparkSession
import org.gft.scalasparksdk.core.constants.Constants

trait GftMainSparkBase extends Constants{

 implicit val spark: SparkSession = SparkSession.builder()
   .master("local[*]")
   .appName(getClass.getSimpleName)
   .enableHiveSupport()
   .config("hive.exec.dynamic.partition", "true")
   .config("spark.sql.sources.partitionOverwriteMode","dynamic")
   .config("hive.exec.dynamic.partition.mode", "nonstrict")
   .getOrCreate()

 final def main(args: Array[String]): Unit = {
  val params = mapParameters(args.mkString(","))

  execute(params)
 }

 def mapParameters(args: String): GlobalParams = {

  args.split(",").toList match {
   case List(step, partitionDate) => GlobalParams(step, partitionDate)
   case List(step) => GlobalParams(step, null)
  }

 }

 def execute(params: GlobalParams)
}