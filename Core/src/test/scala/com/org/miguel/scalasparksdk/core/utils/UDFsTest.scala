package com.org.miguel.scalasparksdk.core.utils

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.util.TimeZone
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.org.miguel.scalasparksdk.core.utils.UDFs._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UDFsTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()
    TimeZone.setDefault(TimeZone.getTimeZone("utc"))
  }

  "timestampToYearStr" should "convert a Timestamp column to a year String" in {
    import spark.sqlContext.implicits._

    val timestampDF = Seq(Timestamp.valueOf("2019-01-01 01:00:01")).toDF("timestamp")

    val actualDF = timestampDF.withColumn("year", timestampToYearStr(col("timestamp")))
      .select("year")

    val expectedDF = Seq("2019").toDF

    actualDF.collect() should contain theSameElementsAs expectedDF.collect()
  }

  "timestampToMonthStr" should "convert a Timestamp column to a year String" in {
    import spark.sqlContext.implicits._

    val timestampDF = Seq(Timestamp.valueOf("2019-10-01 01:00:01")).toDF("timestamp")

    val actualDF = timestampDF.withColumn("month", timestampToMonthStr(col("timestamp")))
      .select("month")

    val expectedDF = Seq("10").toDF

    actualDF.collect() should contain theSameElementsAs expectedDF.collect()
  }

  "timestampToDayStr" should "convert a Timestamp column to a year String" in {
    import spark.sqlContext.implicits._

    val timestampDF = Seq(Timestamp.valueOf("2019-10-31 01:00:01")).toDF("timestamp")

    val actualDF = timestampDF.withColumn("day", timestampToDayStr(col("timestamp")))
      .select("day")

    val expectedDF = Seq("31").toDF

    actualDF.collect() should contain theSameElementsAs expectedDF.collect()
  }

  "strToTimestamp" should "convert a string dateTime column to a Timestamp" in {
    import spark.sqlContext.implicits._

    val stringDateDF = Seq("2019-11-21T09:34:01").toDF("dateString")

    val strToTimestampUDF = strToTimestamp(DateTimeFormatter.ISO_DATE_TIME)

    val actualDF = stringDateDF.withColumn("timestamp", strToTimestampUDF(col("dateString")))
      .select("timestamp")

    val expectedDF = Seq(Timestamp.valueOf("2019-11-21 09:34:01")).toDF

    actualDF.collect() should contain theSameElementsAs expectedDF.collect()
  }

  "dateToYearStr" should "convert a sql Date column to a year String" in {
    import spark.sqlContext.implicits._

    val dateDF = Seq(Date.valueOf("2019-01-01")).toDF("date")

    val actualDF = dateDF.withColumn("year", dateToYearStr(col("date")))
      .select("year")

    val expectedDF = Seq("2019").toDF

    actualDF.collect() should contain theSameElementsAs expectedDF.collect()
  }

  "dateToMonthStr" should "convert a sql Date column to a year String" in {
    import spark.sqlContext.implicits._

    val dateDF = Seq(Date.valueOf("2019-10-01")).toDF("date")

    val actualDF = dateDF.withColumn("month", dateToMonthStr(col("date")))
      .select("month")

    val expectedDF = Seq("10").toDF

    actualDF.collect() should contain theSameElementsAs expectedDF.collect()
  }

  "dateToDayStr" should "convert a sql Date column to a year String" in {
    import spark.sqlContext.implicits._

    val dateDF = Seq(Date.valueOf("2019-08-31")).toDF("date")

    val actualDF = dateDF.withColumn("day", dateToDayStr(col("date")))
      .select("day")

    val expectedDF = Seq("31").toDF

    actualDF.collect() should contain theSameElementsAs expectedDF.collect()
  }
}
