package com.nykytenko.spark

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.UUID

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.unsafe.types.UTF8String

object Functions {

  val createSessionId: UserDefinedFunction = udf(() => UTF8String.fromString(UUID.randomUUID().toString).toString)

  def toDate(format: String): UserDefinedFunction = udf[Timestamp, String]((value: String) => {
    val dateFormat = new SimpleDateFormat(format)

    new Timestamp(dateFormat.parse(value).getTime)
  })
}
