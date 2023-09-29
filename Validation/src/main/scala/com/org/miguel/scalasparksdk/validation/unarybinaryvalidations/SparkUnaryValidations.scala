package com.org.miguel.scalasparksdk.validation.unarybinaryvalidations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, sum, length}
import com.org.miguel.scalasparksdk.validation.SparkValidation
import com.org.miguel.scalasparksdk.validation.SparkValidation.{SuccessMessage, ValidationException}
import org.apache.spark.sql.types.DataType

case class DuplicatesValuesValidation (
  inputDf: DataFrame,
  primaryKey: List[String],
  acceptedDuplicatedValues: Int = 0,
  showingIds: Int = 10
) extends SparkValidation {

  val groupedByPrimaryKey: DataFrame = inputDf.groupBy(primaryKey.map(col): _*).count()

  val duplicatedRows: DataFrame = groupedByPrimaryKey.filter(col("count") > acceptedDuplicatedValues + 1)

  val showingIdsRows: String = duplicatedRows
    .select(primaryKey.map(col): _*)
    .limit(showingIds)
    .collect()
    .map(row => s"[${row.mkString(",")}]").mkString("\n  ")

  val count: Long = duplicatedRows.count()

  @Override
  override val message: Exception =
    if (count > 0) {
      ValidationException(
        s"""DuplicatesValuesValidation validation failed.
           |Found $count duplicated values given the keys:
           |  ${primaryKey.mkString(",")}
           |First $showingIds with duplicated values:
           |  $showingIdsRows
           |""".stripMargin
      )
    } else SuccessMessage("DuplicatesValuesValidation ended successfully")
}

case class NullValuesValidation(
  inputDf: DataFrame,
  primaryKey: List[String],
  acceptedNullValues: Int = 0,
  showingIds: Int = 10
) extends SparkValidation {

  val filterCondition: String = primaryKey.map(item => col(item).isNull).mkString(" or ")

  val filteredDfSize: DataFrame = inputDf.filter(filterCondition)
  val count: Long = filteredDfSize.count()

  val showingIdsRows: String = filteredDfSize
    .select(primaryKey.map(col): _*)
    .limit(showingIds)
    .collect()
    .map(row => s"[${row.mkString(",")}]").mkString("\n  ")

  @Override
  override val message: Exception =
    if (count > acceptedNullValues && count > 0) {
      ValidationException(
        s"""NullValuesValidation validation failed.
           |Found $count null values given the keys:
           |  ${primaryKey.mkString(",")}
           |First $showingIds with null values:
           |  $showingIdsRows
           |""".stripMargin
      )
    } else SuccessMessage("NullValuesValidation ended successfully")
}

case class FieldsDataTypeValidation(
  inputDf: DataFrame,
  primaryKeyTypes: List[(String, DataType)],
  showingIds: Int = 10
) extends SparkValidation {

  val selectedDf: DataFrame = inputDf.select(primaryKeyTypes.map(el => col(el._1)): _*)
  val selectedSchema: List[DataType] = selectedDf.schema.map(item => item.dataType).toList

  val filteredWithSchema: List[DataType] = primaryKeyTypes.map(dataType => dataType._2).filter(selectedSchema.contains(_))

  val count: Int = filteredWithSchema.size

  val incorrectFieldsCount: Int = (count - primaryKeyTypes.size).abs

  val showingIdsRows: String = selectedDf
    .limit(showingIds)
    .collect()
    .map(row => s"[${row.mkString(",")}]").mkString("\n  ")

  @Override
  override val message: Exception =
    if (incorrectFieldsCount != 0) {
      ValidationException(
        s"""FieldsDataTypeValidation validation failed.
           |Found $incorrectFieldsCount different DataTypes given the keys:
           |  ${primaryKeyTypes.map(el => col(el._1)).mkString(",")}
           |First $showingIds with not matching DataTypes columns:
           |  $showingIdsRows
           |""".stripMargin
      )
    } else SuccessMessage("FieldsDataTypeValidation ended successfully")
}

case class CharacterLimitValidation(
  inputDf: DataFrame,
  primaryKey: List[String],
  maxLength: Int,
  showingIds: Int = 10
) extends SparkValidation {

  val filterCondition: String = primaryKey.map(col).map(el => length(el)>maxLength).mkString(" or ")
  val filteredDf: DataFrame = inputDf.select(primaryKey.map(col): _*).filter(filterCondition)

  val count: Long = filteredDf.count

  val showingIdsRows: String = filteredDf
    .limit(showingIds)
    .collect()
    .map(row => s"[${row.mkString(",")}]").mkString("\n  ")

  @Override
  override val message: Exception =
    if (count > 0) {
      ValidationException(
        s"""CharacterLimitValidation validation failed.
           |Found $count values longer than $maxLength given the keys:
           |  ${primaryKey.mkString(",")}
           |First $showingIds with values longer than $maxLength:
           |  $showingIdsRows
           |""".stripMargin
      )
    } else SuccessMessage("CharacterLimitValidation ended successfully")
}

case class NegativeValuesValidation(
  inputDf: DataFrame,
  primaryKey: List[String],
  mustBePositives: Boolean = true,
  showingIds: Int = 10,
  threshold: Double = 0.0
) extends SparkValidation {

  val filterCondition: String = primaryKey
    .map(el => s"${col(el)} ${if (mustBePositives) "<" else ">"} 0" )
    .mkString(" or ")
  val filteredDf: DataFrame = inputDf.filter(filterCondition)
  val filteredCount: Long = filteredDf.count()
  val percentage: Float = filteredCount.toFloat / inputDf.count()

  val showingIdsRows: String = filteredDf
    .select(primaryKey.map(col): _*)
    .limit(showingIds)
    .collect()
    .map(row => s"[${row.mkString(",")}]").mkString("\n  ")

  @Override
  override val message: Exception =
    if (percentage > threshold) {
      ValidationException(
        s"""NegativeValuesValidation validation failed.
           |Found $filteredCount ${if (mustBePositives) "negative" else "positive"} values for the given key:
           |  ${primaryKey.mkString(",")}
           |First $showingIds with ${if (mustBePositives) "negative" else "positive"} values:
           |  $showingIdsRows
           |""".stripMargin
      )
    } else SuccessMessage("NegativeValuesValidation ended successfully")
}

case class ExpectedColumnsValidation(
  inputDf: DataFrame,
  expectedColumns: List[String]
) extends SparkValidation {

  val unexpectedColumns: List[String] = inputDf.columns.toList.filter(!expectedColumns.contains(_))
  val unexpectedCount: Int = unexpectedColumns.size

  val missingColumns: List[String] = expectedColumns.filter(!inputDf.columns.toList.contains(_))
  val missingCount: Int = missingColumns.size

  @Override
  override val message: Exception =
    if (unexpectedCount>0 && missingCount>0) {
      ValidationException(
        s"""ExpectedColumnsValidation validation failed.
           |Found $unexpectedCount unexpected columns:
           |  ${unexpectedColumns.mkString(",")}
           |Found $missingCount missing columns:
           |  ${missingColumns.mkString(",")}
           |""".stripMargin
      )
    }
    else if (unexpectedCount>0 && missingCount==0) {
      ValidationException(
        s"""ExpectedColumnsValidation validation failed.
           |Found $unexpectedCount unexpected columns:
           |  ${unexpectedColumns.mkString(",")}
           |""".stripMargin
      )
    }
    else if (unexpectedCount==0 && missingCount>0) {
      ValidationException(
        s"""ExpectedColumnsValidation validation failed.
           |Found $missingCount missing columns:
           |  ${missingColumns.mkString(",")}
           |""".stripMargin
      )
    }
    else if (!inputDf.columns.toList.equals(expectedColumns)) {
      ValidationException(
        s"""ExpectedColumnsValidation validation failed.
           |Columns order not matching. Expected order:
           |  [${expectedColumns.mkString(", ")}]
           |Current Dataset columns order:
           |  [${inputDf.columns.toList.mkString(", ")}]
           |""".stripMargin
      )
    }
    else SuccessMessage("ExpectedColumnsValidation ended successfully")
}

case class FieldsValuesValidation(
  inputDf: DataFrame,
  primaryKeyValues: List[(String, List[Any])],
  mustBePositives: Boolean = true,
  showingIds: Int = 10
) extends SparkValidation {

  val filterCondition: Column = primaryKeyValues
    .map(item => col(item._1).isin(item._2: _*))
    .map(condition => if (mustBePositives) condition else !condition)
    .reduce(_ and _)
  val filteredDf: DataFrame = inputDf.filter(filterCondition)

  val showingIdsRows: String = filteredDf
    .select(primaryKeyValues.map(el => col(el._1)): _*)
    .limit(showingIds)
    .collect()
    .map(row => s"[${row.mkString(",")}]").mkString("\n  ")
  val filteredCounter: Long = filteredDf.count()

  @Override
  override val message: Exception =
    if (filteredCounter > 0) {
      ValidationException(
        s"""FieldsValuesValidation validation failed.
           |Found $filteredCounter unexpected values given the keys:
           |  ${primaryKeyValues.map(_._1).mkString(",")}
           |First $showingIds with unexpected values:
           |  $showingIdsRows
           |""".stripMargin
      )
    } else SuccessMessage("FieldsValuesValidation ended successfully")
}