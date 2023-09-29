package com.org.miguel.scalasparksdk.testing

import io.delta.tables.DeltaTable

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{DoubleType, StringType}

class SparkTestingTest extends SparkTesting {

  override def beforeAll(): Unit = {
    System.setSecurityManager(null)
    super.beforeAll()
    FileUtils.deleteDirectory(new File("metastore_db/"))
    FileUtils.deleteDirectory(new File("spark-warehouse/"))
    FileUtils.deleteDirectory(new File("target/delta-table/"))
  }

  "loadDataframeFromResource" should "return the actual dataframe from a CSV file - only file name" in {

    val readDf = SparkTesting.loadDataframeFromResource("SparkTestingUtils.csv")

    val expectedColumns = List(
      "column1" -> StringType,
      "column2" -> StringType
    )

    assertResult(1)(readDf.count())
    assertResult(expectedColumns.map(column => column._1))(readDf.columns.toList)
    assertResult(expectedColumns.map(column => column._2))(readDf.schema.fields.map(field => field.dataType))
  }

  it should "return the actual dataframe from a CSV file - file name, directory and data types" in {

    val expectedColumns = List(
      "column1" -> StringType,
      "column2" -> StringType,
      "column3" -> DoubleType
    )

    val readDf = SparkTesting
      .loadDataframeFromResource(
        "testDirectory/SparkTestingUtils.csv",
        expectedColumns
      )

    assertResult(1)(readDf.count())
    assertResult(expectedColumns.map(column => column._1))(readDf.columns.toList)
    assertResult(expectedColumns.map(column => column._2))(readDf.schema.fields.map(field => field.dataType))
  }

  "registerTableFromResource" should "create a temporary table from a file - only fileName" in {
    val tableName = "test.tmpTable_only_fileName"
    SparkTesting.registerTableFromResource("SparkTestingUtils.csv", tableName)

    val expectedColumns = List(
      "column1" -> StringType,
      "column2" -> StringType
    )

    val resultTable = spark.table(tableName)

    assertResult(1)(resultTable.count())
    assertResult(expectedColumns.map(column => column._1))(resultTable.columns.toList)
    assertResult(expectedColumns.map(column => column._2))(resultTable.schema.fields.map(field => field.dataType))
  }

  it should "create a temporary table from a file - file name, directory and data types" in {
    val tableName = "test.tmpTable_fileName_directory_datatype"

    val expectedColumns = List(
      "column1" -> StringType,
      "column2" -> StringType,
      "column3" -> DoubleType
    )

    SparkTesting.registerTableFromResource(
      "testDirectory/SparkTestingUtils.csv",
      tableName,
      expectedColumns
    )

    val resultTable = spark.table(tableName)

    assertResult(1)(resultTable.count())
    assertResult(expectedColumns.map(column => column._1))(resultTable.columns.toList)
    assertResult(expectedColumns.map(column => column._2))(resultTable.schema.fields.map(field => field.dataType))
  }

  "getValuesFromSameId" should "return a list of values for the same given ID" in {
    import spark.implicits._
    val inputDf = List(
      (1, 1),
      (1, 2),
      (2, 1),
      (2, 2)
    ).toDF("id", "testCol")

    assertResult(List(1, 2))(getValuesFromSameId[Int](inputDf, 1, "testCol"))
  }

  "loadDeltaTableFromResource" should "return a Delta Table given a file - only path" in{
    val generatedDeltaTable: DeltaTable = SparkTesting.loadDeltaTableFromResource("SparkTestingUtils.csv")

    val expectedColumns = List(
      "column1" -> StringType,
      "column2" -> StringType
    )

    assertResult(1)(generatedDeltaTable.toDF.count())
    assertResult(expectedColumns.map(column => column._1))(generatedDeltaTable.toDF.columns.toList)
    assertResult(expectedColumns.map(column => column._2))(generatedDeltaTable.toDF.schema.fields.map(field => field.dataType))
  }
}
