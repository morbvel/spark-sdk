package com.org.miguel.scalasparksdk.core.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import com.org.miguel.scalasparksdk.core.constants.Constants

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.util.Try

object DateUtils extends Constants {

  def parseToDate(formatter: DateTimeFormatter, timestampStr: String): Option[LocalDate] =
    Try(LocalDate.parse(timestampStr, formatter)).toOption

  def parseToDateTime(formatter: DateTimeFormatter, timestampStr: String): Option[LocalDateTime] =
    Try(LocalDateTime.parse(timestampStr, formatter)).toOption

  def parseToTimestamp(formatter: DateTimeFormatter, dateTimeString: String): Option[Timestamp] =
    parseToDateTime(formatter, dateTimeString) match {
      case Some(dateTime) => Some(Timestamp.valueOf(dateTime))
      case None => None
    }

  def toYearStr(date: LocalDate): String = date.format(DateTimeFormatter.ofPattern(YEAR_DATE_PATTERN))

  def toYearStr(date: LocalDateTime): String = toYearStr(date.toLocalDate)

  def toYearStr(date: Date): String = toYearStr(date.toLocalDate)

  def toYearStr(timestamp: Timestamp): String = toYearStr(timestamp.toLocalDateTime.toLocalDate)

  def toYearStr(stringDate: String): String = toYearStr(LocalDate.parse(stringDate))

  def toMonthStr(date: LocalDate): String = date.format(DateTimeFormatter.ofPattern(MONTH_DATE_PATTERN))

  def toMonthStr(date: LocalDateTime): String = toMonthStr(date.toLocalDate)

  def toMonthStr(date: Date): String = toMonthStr(date.toLocalDate)

  def toMonthStr(timestamp: Timestamp): String = toMonthStr(timestamp.toLocalDateTime.toLocalDate)

  def toMonthStr(stringDate: String): String = toMonthStr(LocalDate.parse(stringDate))

  def toDayStr(date: LocalDate): String = date.format(DateTimeFormatter.ofPattern(DAY_DATE_PATTERN))

  def toDayStr(date: LocalDateTime): String = toDayStr(date.toLocalDate)

  def toDayStr(date: Date): String = toDayStr(date.toLocalDate)

  def toDayStr(timestamp: Timestamp): String = toDayStr(timestamp.toLocalDateTime.toLocalDate)

  def toDayStr(stringDate: String): String = toDayStr(LocalDate.parse(stringDate))

  def buildDateFilter(dates: List[LocalDate]): Column =

    dates.map(el =>
      col(YEAR_PARTITION).equalTo(toYearStr(el)) &&
      col(MONTH_PARTITION).equalTo(toMonthStr(el)) &&
      col(DAY_PARTITION).equalTo(toDayStr(el))
    ).reduce(_ || _)

}
