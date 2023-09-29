package com.org.miguel.scalasparksdk.core.constants

trait Constants {

  case class GlobalParams(step: String, partitionDate: String)

  val YEAR_DATE_PATTERN: String = "yyyy"
  val MONTH_DATE_PATTERN: String = "MM"
  val DAY_DATE_PATTERN: String = "dd"

  val YEAR_PARTITION: String = "yy"
  val MONTH_PARTITION: String = "mm"
  val DAY_PARTITION: String = "dd"

  val RAW_PREFIX: String = "raw_"
  val HARMONIZATION_PREFIX: String = "harmonization_"
  val FUNCTIONAL_PREFIX: String = "functional_"
  val REPORTING_PREFIX: String = "reporting_"

}
