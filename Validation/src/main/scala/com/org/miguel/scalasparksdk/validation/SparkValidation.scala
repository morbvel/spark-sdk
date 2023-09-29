package com.org.miguel.scalasparksdk.validation

trait SparkValidation {
  val message: Exception
}

object SparkValidation{

  final case class SuccessMessage(private val message: String = "") extends Exception(message)

  final case class ValidationException(private val message: String = "") extends Exception(message)

  def executeValidations(inputValidations: List[SparkValidation]): Boolean = {

    val totalValidations = inputValidations.size.toDouble

    val validationErrorList = inputValidations.map(item => {
      println(item.message.getMessage)
      item.message match {
        case el: SuccessMessage => Some(el).getOrElse(None)
        case el: ValidationException => Some(el).getOrElse(None)
      }
    }).filter(_.getClass.getSimpleName == "ValidationException")

    println(s"\nValidation Results:\n" +
      s"${totalValidations-validationErrorList.size}/$totalValidations validations was successful\n" +
      s"${((totalValidations-validationErrorList.size)/totalValidations)*100}% of succeeded validations\n")

    if(validationErrorList.nonEmpty) {
      throw ValidationException(s"SparkValidation process failed due to ${validationErrorList.size} failing validations")
    }
    true

  }

}
