package com.nykytenko.spark


import java.sql.Timestamp

import cats.effect.Effect
import com.nykytenko.config.CsvConfig
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions._

import scala.collection.mutable


class EtlDescription(
                          sourceDF: DataFrame,
                          transform: DataFrame => DataFrame,
                          write: DataFrame => Array[Row]
                        ) {

  def process[F[_]]()(implicit E: Effect[F]): F[Array[Row]] = E.delay {
    write(sourceDF.transform(transform))
  }
}

class ProcessData(config: CsvConfig, sparkSession: SparkSession) {

  private val toDate = Functions.toDate("yyyy-MM-dd H:m:s")

  object Etls {
    val _1 = new EtlDescription(
      sourceDF = extractDF(config),
      transform = model1(),
      write = dummyWriter()
    )

    val _2 = new EtlDescription(
      sourceDF = extractDF(config),
      transform = model1(),
      write = dummyWriter()
    )

    val _3 = new EtlDescription(
      sourceDF = extractDF(config),
      transform = model1(),
      write = dummyWriter()
    )
  }
  def extractDF(config: CsvConfig): DataFrame = sparkSession
    .read
    .format("csv")
    .options(config.options)
    .csv(config.path)

  private def toCommonResult(df: DataFrame): DataFrame = {
    import sparkSession.implicits._

    val window5min = window(col("sessionTime"), windowDuration = "5 minutes")

    val windowedDS = df
      .filter(!_.anyNull)
      .withColumn("eventTime", toDate(col("eventTime")))
      .withColumnRenamed("eventTime", "sessionTime")
      .withColumn("combined", array($"userId", $"eventType", $"sessionTime", $"product"))
      .groupBy(window5min.alias("window"), $"category")
      .agg(
        min($"sessionTime").alias("sessionStartTime"),
        max($"sessionTime").alias("sessionEndTime"),
        collect_list($"combined").alias("wrappedColumns")
      ).orderBy("category", "sessionEndTime")

    windowedDS
       .withColumn("sessionId", monotonically_increasing_id())
       .withColumn("single", explode(windowedDS.col("wrappedColumns")))
       .withColumn("userId", $"single"(0))
       .withColumn("eventType", $"single"(1))
       .withColumn("eventTime", $"single"(2))
       .withColumn("product", $"single"(3))
      .drop("wrappedColumns", "window", "single")
  }

  // Median Session duration and Users
  private def calcMedianSessionDurationAndGroupUser(df: DataFrame): DataFrame = {
    import sparkSession.implicits._

    df
      .withColumn("timeInSession", unix_timestamp($"eventTime") - unix_timestamp($"sessionStartTime"))
      .withColumn("firstCondition", $"timeInSession" < 60)
      .withColumn("secondCondition", $"timeInSession" >= 60 && $"timeInSession" < 60*5)
      .withColumn("thirdCondition", $"timeInSession" >= 60*5)
      .groupBy("sessionId")
      .agg(
        countDistinct($"userId", when($"firstCondition", true)).alias("less1"),
        countDistinct($"userId", when($"secondCondition", true)).alias("1to5"),
        countDistinct($"userId", when($"thirdCondition", true)).alias("more5"),
        collect_list($"timeInSession").as("timesInSession")
      )
      .map( row =>
        (
          row.getAs[String](0),
          row.getAs[String](1),
          row.getAs[String](2),
          median(row.getAs[mutable.WrappedArray[Double]](3).toList)
        )
      )
      .toDF("sessionIdNew", "less1", "1to5", "more5", "median")  }

  private def top10ProductsByCategory(df: DataFrame): DataFrame = {
    import sparkSession.implicits._

    df
      .withColumn("timeInSession", unix_timestamp($"eventTime") - unix_timestamp($"sessionStartTime"))
      .orderBy("timeInSession")
      .groupBy("sessionId")
      .agg(
        collect_set("product").alias("allProducts")
      )
      .map(row => (
        row.getAs[String]("sessionId"),
        row.getAs[mutable.WrappedArray[String]]("allProducts").take(10))
      )
      .toDF("category", "wrapped")
      .withColumn("product", explode($"wrapped"))
      .drop("wrapped")
  }

  def model1()(df: DataFrame): DataFrame = {
    df
      .transform(toCommonResult)
  }

  def model2()(df: DataFrame): DataFrame = {
    df.transform(toCommonResult)
      .transform(calcMedianSessionDurationAndGroupUser)
  }

  def model3()(df: DataFrame): DataFrame = {
    df.transform(toCommonResult)
      .transform(top10ProductsByCategory)
  }

  def dummyWriter()(df: DataFrame): Array[Row] = df.collect()

  //for median calculation
  private def median(inputList: List[Double]): Double = {
    val count = inputList.size
    if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (inputList(l) + inputList(r)) / 2
    } else
      inputList(count / 2)
  }
}
