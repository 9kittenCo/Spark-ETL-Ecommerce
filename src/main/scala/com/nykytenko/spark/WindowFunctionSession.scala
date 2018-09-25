package com.nykytenko.spark

import java.util.UUID

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.unsafe.types.UTF8String

object WindowFunctionSession {

  private val toDate = Functions.toDate("yyyy-MM-dd H:m:s")

  def sessionizeByCategory(df: DataFrame, maxSessionDuration: Long)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    df
      .withColumn("eventTime", toDate(col("eventTime")))
      .select('userId, 'category, 'eventTime,
        lag('eventTime, 1)
          .over(Window.partitionBy('userId,'category).orderBy('eventTime))
          .as('prevEventTime)
      )
      .select('userId, 'category, 'eventTime,
      when(unix_timestamp($"eventTime") - unix_timestamp($"prevEventTime") < lit(maxSessionDuration), lit(0)).otherwise(lit(1))
          .as('isNewSession))
      .select('userId, 'category, 'eventTime,
        sum('isNewSession)
          .over(Window.partitionBy('userId, 'category).orderBy('eventTime))
          .as('sessionId))
      .groupBy("userId", "category", "sessionId")
      .agg(
        min("eventTime").as("sessionStartTime"),
        max("eventTime").as("sessionEndTime")
      )
      .drop('sessionId)
      .withColumn("sessionId", Functions.createSessionId())
      .orderBy('userId, 'category, 'sessionId)
  }

  def sessionizeByProduct(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    df
      .withColumn("eventTime", toDate(col("eventTime")))
      .select('userId, 'category, 'product, 'eventTime,
        lag('product, 1)
          .over(Window.partitionBy('userId, 'category, 'product).orderBy('eventTime))
          .as('prevProduct)
      )
      .select('userId, 'category, 'product, 'eventTime,
        when('product === 'prevProduct, lit(0)).otherwise(lit(1))
          .as('isNewSession))
      .select('userId, 'category, 'product, 'eventTime,
        sum('isNewSession)
          .over(Window.partitionBy('userId, 'product).orderBy('eventTime))
          .as('sessionId)
      )
     .groupBy("userId", "category", "product", "sessionId")
      .agg(
        min("eventTime").as("sessionStartTime"),
        max("eventTime").as("sessionEndTime")
      )
      .drop('sessionId)
      .withColumn("sessionId", Functions.createSessionId())
//      .orderBy('userId, 'category, 'sessionId)

  }
}
