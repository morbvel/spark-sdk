package com.org.miguel.scalasparksdk.validation

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import com.org.miguel.scalasparksdk.testing.SparkTesting
import com.org.miguel.scalasparksdk.validation.unarybinaryvalidations._

class SparkValidationUnaryTest extends SparkTesting{

  "DuplicatesValuesValidation" should "fail when there are duplicates on a dataframe given a primary key" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = DuplicatesValuesValidation(
      inputDf,
      List("id", "col1")
    )

    assertResult(
      s"""DuplicatesValuesValidation validation failed.
         |Found 2 duplicated values given the keys:
         |  id,col1
         |First 10 with duplicated values:
         |  [2,dummy]
         |  [1,dummy]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "success when no duplicates found" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = DuplicatesValuesValidation(
      inputDf,
      List("id", "col1")
    )

    assertResult("DuplicatesValuesValidation ended successfully")(validation.message.getMessage)
  }

  "NullValuesValidation" should "fail when there are null values given a primary key" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, null, "dummy"),
      (2, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = NullValuesValidation(inputDf, List("id", "col1"))

    assertResult(
      s"""NullValuesValidation validation failed.
         |Found 1 null values given the keys:
         |  id,col1
         |First 10 with null values:
         |  [1,null]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "success when no null values found" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = NullValuesValidation(
      inputDf,
      List("id", "col1")
    )

    assertResult("NullValuesValidation ended successfully")(validation.message.getMessage)
  }

  "FieldsDataTypeValidation" should "fail when there are not matching DataTypes columns given a primary key" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = FieldsDataTypeValidation(
      inputDf,
      List("id" -> IntegerType, "col1" -> DoubleType)
    )

    assertResult(
      s"""FieldsDataTypeValidation validation failed.
         |Found 1 different DataTypes given the keys:
         |  id,col1
         |First 10 with not matching DataTypes columns:
         |  [1,dummy]
         |  [2,dummy]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "success when all columns match DataTypes" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = FieldsDataTypeValidation(
      inputDf,
      List("id" -> IntegerType, "col1" -> StringType)
    )

    assertResult("FieldsDataTypeValidation ended successfully")(validation.message.getMessage)
  }

  "CharacterLimitValidation" should "fail when there are values exceeding the given length given a primary key" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "iiPRk8qhM6abcdefghijk", "dummy"),
      (2, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = CharacterLimitValidation(
      inputDf,
      List("id", "col1"),
      10
    )

    assertResult(
      s"""CharacterLimitValidation validation failed.
         |Found 1 values longer than 10 given the keys:
         |  id,col1
         |First 10 with values longer than 10:
         |  [1,iiPRk8qhM6abcdefghijk]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "success when there are no values exceeding the given length given a primary key" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", null)
    ).toDF("id", "col1", "col2")

    val validation = CharacterLimitValidation(
      inputDf,
      List("id", "col1", "col2"),
      10
    )

    assertResult("CharacterLimitValidation ended successfully")(validation.message.getMessage)
  }

  "NegativeValuesValidation" should "fail when there are negative values given a primary key" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", 1),
      (2, "dummy", 1),
      (3, "dummy", -1)
    ).toDF("id", "col1", "col2")

    val validation = NegativeValuesValidation(
      inputDf,
      List("col2")
    )

    assertResult(
      s"""NegativeValuesValidation validation failed.
         |Found 1 negative values for the given key:
         |  col2
         |First 10 with negative values:
         |  [-1]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "fail when there negative values given a primary key and the flag is set to false" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", 1),
      (2, "dummy", 1),
      (3, "dummy", -1)
    ).toDF("id", "col1", "col2")

    val validation = NegativeValuesValidation(
      inputDf,
      List("col2"),
      mustBePositives = false
    )

    assertResult(
      s"""NegativeValuesValidation validation failed.
         |Found 2 positive values for the given key:
         |  col2
         |First 10 with positive values:
         |  [1]
         |  [1]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "success when all columns match DataTypes" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = NegativeValuesValidation(
      inputDf,
      List("id")
    )

    assertResult("NegativeValuesValidation ended successfully")(validation.message.getMessage)
  }

  it should "success when percentage negative values are below threshold" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (-1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = NegativeValuesValidation(
      inputDf,
      List("id"),
      threshold = 0.9
    )

    assertResult("NegativeValuesValidation ended successfully")(validation.message.getMessage)
  }

  it should "fail when percentage negative values are above threshold" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (-1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = NegativeValuesValidation(
      inputDf,
      List("id"),
      threshold = 0.1
    )

    assertResult(
      s"""NegativeValuesValidation validation failed.
         |Found 1 negative values for the given key:
         |  id
         |First 10 with negative values:
         |  [-1]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  "ExpectedColumnsValidation" should "fail when there are missing columns in the given dataframe" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(1, 2, 3).toDF("id")

    val validation = ExpectedColumnsValidation(
      inputDf,
      List("id", "col1")
    )

    assertResult(
      s"""ExpectedColumnsValidation validation failed.
         |Found 1 missing columns:
         |  col1
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "fail when there are unexpected columns in the given dataframe" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy"),
      (2, "dummy"),
      (3, "dummy")
    ).toDF("id", "col1")

    val validation = ExpectedColumnsValidation(
      inputDf,
      List("id")
    )

    assertResult(
      s"""ExpectedColumnsValidation validation failed.
         |Found 1 unexpected columns:
         |  col1
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "fail when there are missing and unexpected columns in the given dataframe" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy"),
      (2, "dummy"),
      (3, "dummy")
    ).toDF("id", "col1")

    val validation = ExpectedColumnsValidation(
      inputDf,
      List("id", "col2")
    )

    assertResult(
      s"""ExpectedColumnsValidation validation failed.
         |Found 1 unexpected columns:
         |  col1
         |Found 1 missing columns:
         |  col2
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "fail when columns don't match the expected order" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = ExpectedColumnsValidation(
      inputDf,
      List("id", "col2", "col1")
    )

    assertResult(
      s"""ExpectedColumnsValidation validation failed.
         |Columns order not matching. Expected order:
         |  [id, col2, col1]
         |Current Dataset columns order:
         |  [id, col1, col2]
         |""".stripMargin)(validation.message.getMessage)
  }

  it should "success when all columns match the expected" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = ExpectedColumnsValidation(
      inputDf,
      List("id", "col1", "col2")
    )

    assertResult("ExpectedColumnsValidation ended successfully")(validation.message.getMessage)
  }

  "FieldsValuesValidation" should "fail when there are missing columns in the given dataframe" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = FieldsValuesValidation(
      inputDf,
      List("col1" -> List("dummy")),
      mustBePositives = true
    )

    assertResult(
      s"""FieldsValuesValidation validation failed.
         |Found 3 unexpected values given the keys:
         |  col1
         |First 10 with unexpected values:
         |  [dummy]
         |  [dummy]
         |  [dummy]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "success when all columns match the expected" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = FieldsValuesValidation(
      inputDf,
      List("col1" -> List("2")),
      mustBePositives = true
    )

    assertResult("FieldsValuesValidation ended successfully")(validation.message.getMessage)
  }

  it should "success when all columns match the expected and mustBePositives is set to false" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = FieldsValuesValidation(
      inputDf,
      List("col1" -> List("dummy")),
      mustBePositives = false
    )

    assertResult("FieldsValuesValidation ended successfully")(validation.message.getMessage)
  }

  it should "fail when there are unexpected column values and mustBePositives is set to false" in {
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = FieldsValuesValidation(
      inputDf,
      List("col1" -> List("2")),
      mustBePositives = false
    )

    assertResult(
      s"""FieldsValuesValidation validation failed.
         |Found 3 unexpected values given the keys:
         |  col1
         |First 10 with unexpected values:
         |  [dummy]
         |  [dummy]
         |  [dummy]
         |""".stripMargin
    )(validation.message.getMessage)
  }

}
