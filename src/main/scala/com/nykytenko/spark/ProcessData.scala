package com.nykytenko.spark


import java.sql.Timestamp

import cats.effect.Effect
import com.nykytenko.spark.config.CsvConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class DataWithSession(sessionId:String, sessionTime: Timestamp)

case class Result(
                   sessionId: String,
                   sessionStartTime: Timestamp,
                   sessionEndTime: Timestamp
                 )

case class EtlDescription(
                          sourceDF: DataFrame,
                          transform: DataFrame => Dataset[Result],
                          write: Dataset[Result] => Array[Result],
                          metadata: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
                        ) {

  def process(): Array[Result] = {
    write(sourceDF.transform(transform))
  }

}

class ProcessData[F[_]](config: CsvConfig, sparkSession: SparkSession)(implicit E: Effect[F]) {


  val etl: F[EtlDescription] = E.delay {
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
      .select("category", "eventTime")
      .filter(!_.anyNull)
      .withColumnRenamed("category", "sessionId")
      .withColumnRenamed("eventTime", "sessionTime")
      .withColumn("sessionTime", $"sessionTime".cast("timestamp"))
      .as[DataWithSession]
  }

  private def toResult(ds: Dataset[DataWithSession]): Dataset[Result] = {
    import sparkSession.implicits._

    val window5min = window(col("sessionTime"), windowDuration = "5 minutes")

    ds
      .groupBy(col("sessionId"), window5min)
      .agg(
        max(col("sessionTime")).alias("sessionEndTime"),
        min(col("sessionTime")).alias("sessionStartTime")
      ).orderBy("sessionId")
      .as[Result]
  }

  private def toResultWd(ds: Dataset[DataWithSession]): Dataset[Result] = {
    import sparkSession.implicits._
    import org.apache.spark.sql.expressions.Window

    val windowSpec = Window.partitionBy("sessionId")
      .orderBy(col("sessionTime").cast("long"))
      .rangeBetween(-150, 150)

    ds
//      .groupBy("sessionId")
//      .agg(
//        max(col("sessionTime")).alias("sessionEndTime"),
//        min(col("sessionTime")).alias("sessionStartTime")
//      ).orderBy("sessionId")
      .withColumn("sessionTime", last("sessionTime").over(windowSpec))
      .withColumn("sessionStartTime", $"sessionTime".cast("timestamp"))
      .withColumn("sessionEndTime", $"sessionTime".cast("timestamp"))
      .orderBy("sessionId", "sessionStartTime", "sessionEndTime")
      .as[Result]
  }

  def calcMedianSessionDuration() = ???

  def findUsersPerCategory() = ???

  def top10ProductsByCategory() = ???

  private def model()(df: DataFrame): Dataset[Result] = {
    df
      .transform(toSessionInfo)
//      .transform(toResult)
      .transform(toResultWd)

  }

  private def dummyWriter()(ds: Dataset[Result]): Array[Result] = ds.collect()
}
