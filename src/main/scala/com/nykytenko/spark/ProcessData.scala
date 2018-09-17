package com.nykytenko.spark


import java.sql.Timestamp

import cats.effect.Effect
import com.nykytenko.spark.config.CsvConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


case class DataWithSession(sessionId: String,
                           product: String,
                           userId: String,
 //                          sessionId: String,
                           sessionTime: Timestamp,
                           eventType: String
                          )

//case class ResultingSet(
//                   window: Window5Min,
//                   sessionId: String,
//                   sessionStartTime: Timestamp,
//                   sessionEndTime: Timestamp,
//                   wrappedColumns: Seq[WrappedColumns]
//                 )
case class ResultingSet(
                         sessionStartTime: Timestamp,
                         sessionId: String,
                         window: Window5Min,
                         eventTime: Array[String],
                         eventType: String,
                         sessionEndTime: Timestamp,
                         userId: String
                       )
case class Window5Min(start: Timestamp, end: Timestamp)

case class WrappedColumns(
                           userId: String,
                           eventType: String,
                           sessionTime: Timestamp
                         )

case class EtlDescription(
                          sourceDF: DataFrame,
                          transform: DataFrame => Dataset[ResultingSet],
                          write: Dataset[ResultingSet] => Array[ResultingSet],
                          metadata: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
                        ) {

  def process(): Array[ResultingSet] = {
    write(sourceDF.transform(transform))
  }
}

class ProcessData[F[_]](config: CsvConfig, sparkSession: SparkSession)(implicit E: Effect[F]) {

  private val toDate = Functions.toDate("yyyy-MM-dd H:m:s")
//  val toWrappedColumns = udf((arrayCol:Array[(String, String, Timestamp)]) => WrappedColumns(arrayCol.))

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
      .withColumnRenamed("category", "sessionId")
      .withColumnRenamed("eventTime", "sessionTime")
      .as[DataWithSession]
  }

  private def toResult(ds: Dataset[DataWithSession]): Dataset[ResultingSet] = {
    import sparkSession.implicits._

    val window5min = window(col("sessionTime"), windowDuration = "5 minutes")

    val r = ds
        .withColumn("sessionTimeS", $"sessionTime".cast("string"))
      .withColumn("combined", array($"userId", $"eventType", $"sessionTimeS"))//
      .drop($"sessionTimeS")
//            .withColumn("combined", concat_ws(", ", col("userId"),  col("eventType"), col("sessionTime")))
      .groupBy(
      window5min.alias("window"),
      $"sessionId"
    )
      .agg(
        min($"sessionTime").alias("sessionStartTime"),
        max($"sessionTime").alias("sessionEndTime"),
        collect_list($"combined").alias("wrappedColumns")
      ).orderBy("sessionId", "sessionEndTime")

     r
       .withColumn("userId", $"wrappedColumns"(0))
       .withColumn("eventType", $"wrappedColumns" (1))
       .withColumn("eventTime", $"wrappedColumns" (2))
 //      .withColumn("eventTime", toDate(col("eventTime")))
       .drop($"wrappedColumns")
       .as[ResultingSet]
  }

  def calcMedianSessionDuration()(ds: Dataset[ResultingSet]) = {
    ds
      .withColumn("sessionDuration", col("sessionEndTime") - col("sessionStartTime"))
      .stat.approxQuantile("sessionDuration", Array(0.5), 0.25)
  }

  def findUsersPerCategory() = ???

  def top10ProductsByCategory() = ???

  private def model()(df: DataFrame): Dataset[ResultingSet] = {
    df
      .transform(toSessionInfo)
      .transform(toResult)
  }

  private def dummyWriter()(ds: Dataset[ResultingSet]): Array[ResultingSet] = ds.collect()
}
