package com.org.miguel.scalasparksdk.validation.unarybinaryvalidations

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{col, sum}
import com.org.miguel.scalasparksdk.validation.SparkValidation
import com.org.miguel.scalasparksdk.validation.SparkValidation.{SuccessMessage, ValidationException}

import scala.collection.immutable.NumericRange
import scala.math._

case class MissingValuesValidation(
  leftDf: DataFrame,
  rightDf: DataFrame,
  primaryKey: List[String],
  acceptedMissingValues: Int = 0,
  showingIds: Int = 10
) extends SparkValidation {

  val filterCondition: List[Column] = primaryKey.map(el => leftDf(el).isNotNull and rightDf(el).isNull)

  val outerJoin: DataFrame = leftDf.join(
    rightDf,
    primaryKey,
    "left_anti"
  )

  val showingIdsRows: String = outerJoin
    .select(primaryKey.map(col): _*)
    .limit(showingIds)
    .collect()
    .map(row => s"[${row.mkString(",")}]").mkString("\n ")

  val outerCounter: Long = outerJoin.count()

  override val message: Exception =
    if (outerCounter > acceptedMissingValues && outerCounter > 0){
      ValidationException(
        s"""MissingValuesValidation validation failed.
           |Found $outerCounter not matching values in both dataframes for the keys:
           | ${primaryKey.mkString(",")}
           |First $showingIds not matching values in both dataframes:
           | $showingIdsRows
           |""".stripMargin
      )
    } else SuccessMessage("MissingValuesValidation ended successfully")
}

case class PercentageRangeDifferenceValidation(
  leftDf: DataFrame,
  rightDf: DataFrame,
  acceptedPercentage: Double = 0.25
) extends SparkValidation{

  val leftCount: Long = leftDf.count
  val rightCount: Long = rightDf.count

  val range: NumericRange.Inclusive[BigDecimal] =
    BigDecimal(
      ceil(leftCount - (leftCount*acceptedPercentage))
    ) to BigDecimal(
      floor(leftCount + (leftCount*acceptedPercentage))
    ) by 1

  val percentageChange: Double = (rightCount - leftCount) / leftCount

  override val message: Exception =
    if (!range.contains(BigDecimal(rightCount))) {
      ValidationException(
        s"""PercentageRangeDifferenceValidation validation failed.
           |Found ${leftCount - rightCount} missing rows in both Dataframes
           |""".stripMargin
      )
    } else SuccessMessage("PercentageRangeDifferenceValidation ended successfully")
}


case class BinaryDuplicatedValuesValidation(
  leftDf: DataFrame,
  rightDf: DataFrame,
  primaryKey: List[String],
  acceptedDuplicatedValue: Int = 0,
  showingIds: Int = 10
) extends SparkValidation {

  val leftDuplicates: DataFrame = leftDf.groupBy(primaryKey.map(col): _*).count.filter(col("count")>1)
  val rightDuplicates: DataFrame = rightDf.groupBy(primaryKey.map(col): _*).count().filter(col("count")>1)

  val leftDuplicatesCount: Long = leftDuplicates.count
  val rightDuplicatesCount: Long = rightDuplicates.count

  val showingIdsRows: String = rightDuplicates
    .limit(showingIds)
    .collect()
    .map(row => s"[${row.mkString(",")}]").mkString("\n ")

  val primaryKeyDuplicates: List[String] = primaryKey:::List("counter")

  override val message: Exception = {
    if ((leftDuplicatesCount != rightDuplicatesCount) && rightDuplicatesCount != acceptedDuplicatedValue){
      ValidationException(
        s"""BinaryDuplicatedValuesValidation validation failed.
           |Found $rightDuplicatesCount duplicated values for the keys:
           | ${primaryKeyDuplicates.mkString(",")}
           |First $showingIds duplicated values:
           | $showingIdsRows
           |""".stripMargin
      )
    } else SuccessMessage("BinaryDuplicatedValuesValidation ended successfully")
  }
}

case class BinaryIncludesDataframeValidation(
  largeDf: DataFrame,
  smallDf: DataFrame,
  primaryKey: List[String]
) extends SparkValidation {

  val leftAntiJoin: DataFrame = smallDf.join(
    largeDf,
    primaryKey,
    "left_anti"
  )

  val leftAntiCount: Long = leftAntiJoin.count

  override val message: Exception = {
    if (leftAntiCount  > 0){
      ValidationException(
        s"""BinaryIncludesDataframeValidation validation failed.
           |Found $leftAntiCount missing values in the large dataframe compared with the small""".stripMargin
      )
    } else SuccessMessage("BinaryIncludesDataframeValidation ended successfully")
  }
}
