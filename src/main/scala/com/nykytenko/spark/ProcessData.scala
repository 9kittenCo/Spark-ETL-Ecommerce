package com.nykytenko.spark


import java.sql.Timestamp

import cats.effect.Effect
import com.nykytenko.spark.config.CsvConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


case class DataWithSession(category: String,
                           product: String,
                           userId: String,
                           sessionTime: Timestamp,
                           eventType: String
                          )

case class Result(
                         sessionId: String,
                         sessionStartTime: Timestamp,
                         sessionEndTime: Timestamp,
                         userId: String,
                         eventType: String,
                         eventTime: Timestamp
                       )

case class EtlDescription(
                          sourceDF: DataFrame,
                          transform: DataFrame => Dataset[Result],
                          write: Dataset[Result] => Array[Result]
                        ) {

  def process(): Array[Result] = {
    write(sourceDF.transform(transform))
  }
}

class ProcessData[F[_]](config: CsvConfig, sparkSession: SparkSession)(implicit E: Effect[F]) {

  private val toDate = Functions.toDate("yyyy-MM-dd H:m:s")

  val etl1: F[EtlDescription] = E.delay {
    EtlDescription(
      sourceDF = extractDF(config),
      transform = model(),
      write = dummyWriter()
    )
  }

  def extractDF(config: CsvConfig): DataFrame = sparkSession
    .read
    .format("csv")
    .options(config.options)
    .csv(config.path)

  private def toSessionInfo(df: DataFrame): Dataset[DataWithSession] = {
    import sparkSession.implicits._

    df
      .filter(!_.anyNull)
      .withColumn("eventTime", toDate(col("eventTime")))
      .withColumnRenamed("eventTime", "sessionTime")
      .as[DataWithSession]
  }

  private def toResult(ds: Dataset[DataWithSession]): Dataset[Result] = {
    import sparkSession.implicits._

    val window5min = window(col("sessionTime"), windowDuration = "5 minutes")

    val r = ds
      .withColumn("combined", array($"userId", $"eventType", $"sessionTime"))
      .groupBy(window5min.alias("window"), $"category")
      .agg(
        min($"sessionTime").alias("sessionStartTime"),
        max($"sessionTime").alias("sessionEndTime"),
        collect_list($"combined").alias("wrappedColumns")
      ).orderBy("category", "sessionEndTime")

     r
       .withColumn("sessionId", monotonically_increasing_id())
       .withColumn("single", explode(r.col("wrappedColumns")))
       .withColumn("userId", $"single"(0))
       .withColumn("eventType", $"single"(1))
       .withColumn("eventTime", $"single"(2))
       .drop("wrappedColumns", "window", "single")
       .as[Result]
  }

  def calcMedianSessionDuration()(ds: Dataset[Result]) = {
    ds
      .withColumn("sessionDuration", col("sessionEndTime") - col("sessionStartTime"))
      .stat.approxQuantile("sessionDuration", Array(0.5), 0.25)
  }

  def findUsersPerCategory() = ???

  def top10ProductsByCategory() = ???

  private def model()(df: DataFrame): Dataset[Result] = {
    df
      .transform(toSessionInfo)
      .transform(toResult)
  }

  private def dummyWriter()(ds: Dataset[Result]): Array[Result] = ds.collect()
}
