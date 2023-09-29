package com.org.miguel.scalasparksdk.validation

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import com.org.miguel.scalasparksdk.testing.SparkTesting
import com.org.miguel.scalasparksdk.validation.unarybinaryvalidations._

class SparkValidationMainTest extends SparkTesting{

  "executeValidations" should "fail -throw an exception- when one or more validation cases fail" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2").cache()

    val validations = List(
      DuplicatesValuesValidation(inputDf, List("col1", "col2")),
      DuplicatesValuesValidation(inputDf, List("id")),
      NullValuesValidation(inputDf, List("id", "col1")),
      FieldsDataTypeValidation(inputDf, List("id" -> IntegerType, "col1" -> DoubleType)),
      CharacterLimitValidation(inputDf, List("id", "col1"), 2),
      NegativeValuesValidation(inputDf, List("id"), mustBePositives = false),
      ExpectedColumnsValidation(inputDf, List("id")),
      FieldsValuesValidation(inputDf, List("col1" -> List("dummy")))
    )

    val exception = intercept[SparkValidation.ValidationException]{ SparkValidation.executeValidations(validations) }

    assertResult("SparkValidation process failed due to 6 failing validations")(exception.getMessage)
  }

  it should "end successfully -return true- when no failed validations occur" in{
    val spark = this.spark
    import spark.implicits._

    val inputDf = List(
      (1, "dummy", "dummy"),
      (2, "dummy", "dummy"),
      (3, "dummy", "dummy")
    ).toDF("id", "col1", "col2").cache()

    val validations = List(
      DuplicatesValuesValidation(inputDf, List("id")),
      NullValuesValidation(inputDf, List("id", "col1")),
      FieldsDataTypeValidation(inputDf, List("id" -> IntegerType, "col1" -> StringType)),
      CharacterLimitValidation(inputDf, List("id", "col1"), 10),
      NegativeValuesValidation(inputDf, List("id")),
      ExpectedColumnsValidation(inputDf, List("id", "col1", "col2")),
      FieldsValuesValidation(inputDf, List("col1" -> List("NotAllowed")))
    )

    val results = SparkValidation.executeValidations(validations)

    assertResult(true)(results)
  }

}
