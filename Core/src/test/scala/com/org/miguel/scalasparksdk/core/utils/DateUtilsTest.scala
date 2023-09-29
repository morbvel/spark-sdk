package com.org.miguel.scalasparksdk.core.utils

import com.org.miguel.scalasparksdk.core.utils.DateUtils._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

class DateUtilsTest extends AnyFlatSpec with Matchers{

  "parseToDate" should "parse a String date to a LocalDate" in {
    val strDate = "2019-04-13"

    val actualDate = parseToDate(DateTimeFormatter.ISO_DATE, strDate)
    val expectedDate = Some(LocalDate.of(2019, 4, 13))

    actualDate should equal(expectedDate)
  }

  "parseToDateTime" should "parse a String datetime to a LocalDateTime" in {
    val strDateTime = "2019-08-31T12:04:33"

    val actualDateTime = parseToDateTime(DateTimeFormatter.ISO_DATE_TIME, strDateTime)
    val expectedDateTime =
      Some(LocalDateTime.of(2019, 8, 31, 12, 4, 33))

    actualDateTime should equal(expectedDateTime)
  }

  "parseToTimestamp" should "parse a String datetime to a LocalDate" in {
    val strDateTime = "2019-11-01T00:37:12"

    val actualTimestamp = parseToTimestamp(DateTimeFormatter.ISO_DATE_TIME, strDateTime)
    val expectedTimestamp = Some(Timestamp.valueOf("2019-11-01 00:37:12"))

    actualTimestamp should equal(expectedTimestamp)
  }

  "parseToTimestamp" should "return None for an unparseable String datetime" in {
    val strDateTime = "2019-11-011111111T00:37:12"

    val actualTimestamp = parseToTimestamp(DateTimeFormatter.ISO_DATE_TIME, strDateTime)

    actualTimestamp should be(None)
  }

  "toYearStr" should "format a LocalDate to a String containing the year" in {
    val date = LocalDate.of(2019, 1, 24)

    val actual = toYearStr(date)
    val expected = "2019"

    actual should equal(expected)
  }

  "toYearStr" should "format a LocalDateTime to a String containing the year" in {
    val date = LocalDateTime.of(2018, 11, 1, 10, 33, 1)

    val actual = toYearStr(date)
    val expected = "2018"

    actual should equal(expected)
  }

  "toYearStr" should "format a sql Date to a String containing the year" in {
    val date = Date.valueOf("2019-01-24")

    val actual = toYearStr(date)
    val expected = "2019"

    actual should equal(expected)
  }

  "toMonthStr" should "format a LocalDate to a String containing the month" in {
    val date = LocalDate.of(2019, 1, 24)

    val actual = toMonthStr(date)
    val expected = "01"

    actual should equal(expected)
  }

  "toMonthStr" should "format a LocalDateTime to a String containing the month" in {
    val date = LocalDateTime.of(2018, 11, 1, 10, 33, 1)

    val actual = toMonthStr(date)
    val expected = "11"

    actual should equal(expected)
  }

  "toMonthStr" should "format a sql Date to a String containing the month" in {
    val date = Date.valueOf("2019-01-24")

    val actual = toMonthStr(date)
    val expected = "01"

    actual should equal(expected)
  }

  "toDayStr" should "format a LocalDate to a String containing the day" in {
    val date = LocalDate.of(2019, 1, 24)

    val actual = toDayStr(date)
    val expected = "24"

    actual should equal(expected)
  }

  "toDayStr" should "format a LocalDateTime to a String containing the day" in {
    val date = LocalDateTime.of(2018, 11, 1, 10, 33, 1)

    val actual = toDayStr(date)
    val expected = "01"

    actual should equal(expected)
  }

  "toDayStr" should "format a sql Date to a String containing the day" in {
    val date = Date.valueOf("2019-01-24")

    val actual = toDayStr(date)
    val expected = "24"

    actual should equal(expected)
  }

  "buildDateFilter" should "return a Spark SQL query to filter where partition date matches a provided date" in {
    val dates = List(
      LocalDate.of(2020, 1, 1),
      LocalDate.of(2019, 12, 31))

    val actual = buildDateFilter(dates).toString()
    val expected = "((((yy = 2020) AND (mm = 01)) AND (dd = 01)) OR (((yy = 2019) AND (mm = 12)) AND (dd = 31)))"

    actual should equal(expected)
  }

}
