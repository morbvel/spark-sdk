package com.org.miguel.scalasparksdk.core.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.org.miguel.scalasparksdk.core.constants.Constants

import scala.annotation.tailrec

object TableUtils extends Constants{

  def readFromTable(
    prefix: String,
    database: String,
    tableName: String)
  (implicit spark: SparkSession): DataFrame = {

    spark
      .read
      .option("inferSchema", "true")
      .option("header", "false")
      .table(s"${prefix}_$database.$tableName")
  }


  def writeToTable(
    prefix: String,
    database: String,
    tableName: String,
    partitions: List[String] = Nil,
    format: String = "parquet",
    mode: SaveMode = SaveMode.Overwrite
  )(dfToWrite: DataFrame): DataFrame = {

    dfToWrite
      .write
      .partitionBy(partitions: _*)
      .mode(mode)
      .option("inferSchema", "true")
      .option("header", "false")
      .format(format)
      .saveAsTable(s"${prefix}$database.$tableName")

    dfToWrite
  }

  def writeAndReadTable(
    prefix: String,
    database: String,
    tableName: String,
    partitions: List[String] = Nil,
    format: String = "parquet")
  (dfToWriteAndRead: DataFrame)
  (implicit spark: SparkSession): DataFrame = {

    dfToWriteAndRead.transform(
      writeToTable(prefix, database, tableName, partitions, format)
    )

    readFromTable(prefix, database, tableName)
  }

  def readFromBigquery(datasetName: String, tableName: String)(implicit spark: SparkSession): DataFrame =

    spark
      .read.format("bigquery")
      .option("table", s"$datasetName.$tableName")
      .load()

  def writeToBigquery(datasetName: String, tableName: String, mode: SaveMode = SaveMode.Append)
    (inputDf: DataFrame)(implicit spark: SparkSession): DataFrame = {

    inputDf
      .write.format("bigquery")
      .mode(mode)
      .option("table", s"$datasetName.$tableName")
      .option("temporaryGcsBucket", "data-asset-europe-west2-datalake/tables_to_bigquery")
      .save()

    inputDf
  }

  def selectCastColumns(colsToCast: List[(String, DataType)])(dfToCast: DataFrame): DataFrame = {

    @tailrec
    def loop(cols: List[(String, DataType)] = colsToCast, inputDf: DataFrame = dfToCast): DataFrame = {
      cols match{
        case Nil => inputDf
        case head::tail => loop(tail, inputDf.withColumn( head._1, col(head._1).cast(head._2) ))
      }
    }
    loop().select(colsToCast.map(el => col(el._1)): _*)

  }

  def getSchemaFromJson(jsonPath: String): StructType =
    DataType.fromJson(scala.io.Source.fromFile(jsonPath).mkString).asInstanceOf[StructType]

}
