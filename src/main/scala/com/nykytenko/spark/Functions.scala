package com.nykytenko.spark

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Functions {
  def toDouble(delimiter: String): UserDefinedFunction = udf[Double, String](_.replace(delimiter, ".").toDouble)

  def toDate(format: String): UserDefinedFunction = udf[Timestamp, String]((value: String) => {
    val dateFormat = new SimpleDateFormat(format)

    new Timestamp(dateFormat.parse(value).getTime)
  })
}
