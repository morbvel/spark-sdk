package com.org.miguel.scalasparksdk.core.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter

import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream

object UDFs {

  val timestampToYearStr: UserDefinedFunction = udf((timestamp: Timestamp) => DateUtils.toYearStr(timestamp))

  val timestampToMonthStr: UserDefinedFunction = udf((timestamp: Timestamp) => DateUtils.toMonthStr(timestamp))

  val timestampToDayStr: UserDefinedFunction = udf((timestamp: Timestamp) => DateUtils.toDayStr(timestamp))

  def strToTimestamp(formatter: DateTimeFormatter): UserDefinedFunction =
    udf((dateTimeStr: String) => DateUtils.parseToTimestamp(formatter, dateTimeStr))

  val dateToYearStr: UserDefinedFunction = udf((date: Date) => DateUtils.toYearStr(date))

  val dateToMonthStr: UserDefinedFunction = udf((date: Date) => DateUtils.toMonthStr(date))

  val dateToDayStr: UserDefinedFunction = udf((date: Date) => DateUtils.toDayStr(date))

  val stringToYearStr: UserDefinedFunction = udf((date: String) => DateUtils.toYearStr(date))

  val stringToMonthStr: UserDefinedFunction = udf((date: String) => DateUtils.toMonthStr(date))

  val stringToDayStr: UserDefinedFunction = udf((date: String) => DateUtils.toDayStr(date))

  val bytesToString = udf((bytes: Array[Byte]) => {
    val inputStream = new GZIPInputStream(new ByteArrayInputStream(bytes))
    scala.io.Source.fromInputStream(inputStream).mkString
  })

}
