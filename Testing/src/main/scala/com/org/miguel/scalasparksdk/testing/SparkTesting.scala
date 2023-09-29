package com.org.miguel.scalasparksdk.testing

import java.nio.file.Paths
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import io.delta.tables.DeltaTable
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType
import org.scalatest.flatspec.AnyFlatSpec

trait SparkTesting extends AnyFlatSpec with BeforeAndAfterAll with Matchers{

  val path = Paths.get("").toAbsolutePath.toString + "/spark-warehouse"

  implicit val spark: SparkSession = SparkSession.builder()
    .config("spark.sql.warehouse.dir", path) // fix for bug in windows
    .config("spark.ui.enabled", value = false)
    .config("spark.driver.host", "localhost")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .appName(getClass.getSimpleName)
    .enableHiveSupport()
    .getOrCreate()

  def getValueFromId[T](inputDf: DataFrame, id: Int, colToTest: String): T =
    inputDf.filter(col("id").equalTo(id)).head.getAs[T](colToTest)

  def getValuesFromSameId[T](inputDf: DataFrame, id: Int, colToTest: String): List[T] =
    inputDf
      .filter(col("id").equalTo(id))
      .select(colToTest)
      .rdd.map(row => row(0))
      .collect().map(_.asInstanceOf[T]).toList
}

object SparkTesting{

  def loadDataframeFromResource(fileName: String)(implicit spark: SparkSession): DataFrame =
    loadDataframeFromResource(fileName, typeConversions = Nil)


  def loadDataframeFromResource(
    fileName: String,
    typeConversions: List[(String, DataType)]
  )(implicit spark: SparkSession): DataFrame = {

    val resourcePath: String = s"/${fileName}".replaceAll("/+", "/")
    val resource = getClass.getResource(resourcePath)
    val resolvedPath = resource match {
      case null => throw new InvalidResourceRequestException(s"Resource location: ${resourcePath} not found")
      case _ => resource.getPath
    }

    val fileType = resolvedPath.split("\\.").last

    val df = spark.read
      .format(fileType)
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .load(resolvedPath)

    applyConversions(df, typeConversions)
  }

  def registerTableFromResource(fileName: String, tableName: String)(implicit spark: SparkSession): Unit =
    registerTableFromResource(fileName, tableName, typeConversions = Nil)

  def registerTableFromResource(
    fileName: String,
    tableName: String,
    typeConversions: List[(String, DataType)]
  )(implicit spark: SparkSession): Unit = {

    val separatedName = tableName.split("""\.""")
    val database = separatedName.toList match {
      case List(db, _) => Some(db)
      case List(_) => None
      case _ => throw new IllegalArgumentException(
        "table name must be in format [database.]tableName"
      )
    }

    if(database.isDefined) spark.sql(s"CREATE DATABASE IF NOT EXISTS ${database.get}")

    loadDataframeFromResource(fileName, typeConversions).write.saveAsTable(tableName)
  }

  def loadDeltaTableFromResource(fileName: String)(implicit spark: SparkSession): DeltaTable =
    loadDeltaTableFromResource(fileName, typeConversions = Nil)

  def loadDeltaTableFromResource(
    fileName: String,
    typeConversions: List[(String, DataType)]
  )(implicit spark: SparkSession): DeltaTable = {

    val resourcePath: String = s"/${fileName}".replaceAll("/+", "/")

    val deltaName = resourcePath.split("\\.").head

    loadDataframeFromResource(fileName, typeConversions)
      .write
      .format("delta")
      .save(s"target/delta-table/$deltaName")

    DeltaTable.forPath(spark, s"target/delta-table/$deltaName")
  }

  private def applyConversions(df: DataFrame, typeConversions: List[(String, DataType)]): DataFrame =

    typeConversions.foldLeft[DataFrame](df){
      (df, typeConversions) => df.withColumn(typeConversions._1, col(typeConversions._1).cast(typeConversions._2))
    }
}