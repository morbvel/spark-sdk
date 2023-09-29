package com.org.miguel.scalasparksdk.validation

import com.org.miguel.scalasparksdk.testing.SparkTesting
import com.org.miguel.scalasparksdk.validation.unarybinaryvalidations._

class SparkValidationBinaryTest extends SparkTesting{

  "MissingValuesValidation" should "fail when there are not matching values in both dataframes given a primary key" in{
    val spark = this.spark
    import spark.implicits._

    val leftDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val rightDf = List(
      (1, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = MissingValuesValidation(
      leftDf,
      rightDf,
      List("id", "col1", "col2")
    )

    assertResult(
      s"""MissingValuesValidation validation failed.
         |Found 2 not matching values in both dataframes for the keys:
         | id,col1,col2
         |First 10 not matching values in both dataframes:
         | [2,dummy,dummy]
         | [3,dummy,dummy]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "success when no missing found" in {
    val spark = this.spark
    import spark.implicits._

    val leftDf = List(
      (1, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val rightDf = List(
      (1, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = MissingValuesValidation(
      leftDf,
      rightDf,
      List("id", "col1", "col2")
    )

    assertResult("MissingValuesValidation ended successfully")(validation.message.getMessage)
  }

  it should "fail when comparing empty dataframes" in {
    val spark = this.spark
    import spark.implicits._

    val leftDf = List(
      (1, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val rightDf = Seq.empty[(Int, String, String)].toDF("id", "col1", "col2")

    val validation = MissingValuesValidation(
      leftDf,
      rightDf,
      List("id", "col1", "col2")
    )

    assertResult(
      s"""MissingValuesValidation validation failed.
         |Found 1 not matching values in both dataframes for the keys:
         | id,col1,col2
         |First 10 not matching values in both dataframes:
         | [1,dummy,dummy]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  "BinaryDuplicatedValuesValidation" should "fail when there are duplicated values given a primary key compared to both dataframe" in{
    val spark = this.spark
    import spark.implicits._

    val leftDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val rightDf = List(
      (1, "dummy", "dummy"),
      (1, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = BinaryDuplicatedValuesValidation(
      leftDf,
      rightDf,
      List("id", "col1", "col2")
    )

    assertResult(
      s"""BinaryDuplicatedValuesValidation validation failed.
         |Found 1 duplicated values for the keys:
         | id,col1,col2,counter
         |First 10 duplicated values:
         | [1,dummy,dummy,2]
         |""".stripMargin
    )(validation.message.getMessage)
  }

  it should "success when no duplicates found" in {
    val spark = this.spark
    import spark.implicits._

    val leftDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val rightDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = BinaryDuplicatedValuesValidation(
      leftDf,
      rightDf,
      List("id", "col1", "col2")
    )

    assertResult("BinaryDuplicatedValuesValidation ended successfully")(validation.message.getMessage)
  }

  "BinaryIncludesDataframeValidation" should "fail when all values from a small Dataframe are not included in a larger one" in{
    val spark = this.spark
    import spark.implicits._

    val leftDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy"),
      (4, "dummy", "dummy"),
      (5, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val rightDf = List(
      (1, "dummy", "dummy"),
      (10, "Not_included", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = BinaryIncludesDataframeValidation(
      leftDf,
      rightDf,
      List("id", "col1", "col2")
    )

    assertResult(
      s"""BinaryIncludesDataframeValidation validation failed.
         |Found 1 missing values in the large dataframe compared with the small""".stripMargin
    )(validation.message.getMessage)
  }

  it should "success when there are no missing values" in {
    val spark = this.spark
    import spark.implicits._

    val leftDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy"),
      (4, "dummy", "dummy"),
      (5, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val rightDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy")
    ).toDF("id", "col1", "col2")

    val validation = BinaryIncludesDataframeValidation(
      leftDf,
      rightDf,
      List("id", "col1", "col2")
    )

    assertResult("BinaryIncludesDataframeValidation ended successfully")(validation.message.getMessage)
  }

}
