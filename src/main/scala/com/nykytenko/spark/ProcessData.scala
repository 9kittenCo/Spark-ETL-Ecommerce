package com.nykytenko.spark

import cats.effect.Effect
import com.nykytenko.config.CsvConfig
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions._

import scala.collection.mutable


class EtlDescription(
                      sourceDF: DataFrame,
                      transform: DataFrame => DataFrame,
                      write: DataFrame => Unit
                    ) {
  def process[F[_]]()(implicit E: Effect[F]): F[Unit] = E.delay {
    write(sourceDF.transform(transform))
  }
}

class ProcessData(config: CsvConfig)(implicit sparkSession: SparkSession) {

  val mapEtl: Map[String, EtlDescription] = Map[String, EtlDescription](
    "_1" -> new EtlDescription(sourceDF = extractDF(config), transform = model1(), write = dummyWriter()),
            "_2" -> new EtlDescription(sourceDF = extractDF(config), transform = model2(), write = dummyWriter()),
            "_3" -> new EtlDescription(sourceDF = extractDF(config), transform = model3(), write = dummyWriter())
  )

  def extractDF(config: CsvConfig): DataFrame = sparkSession
    .read
    .format("csv")
    .options(config.options)
    .csv(config.path)

  private def toCommonResult(df: DataFrame): DataFrame = {

    import WindowFunctionSession._

    sessionizeByCategory(df, maxSessionDuration = 300)
  }

  // Median Session duration and Users
  private def calcMedianSessionDurationAndGroupUser(df: DataFrame): DataFrame = {
    import sparkSession.implicits._

    df
      .withColumn("timeInSession", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))
      .withColumn("firstCondition", $"timeInSession" < 60)
      .withColumn("secondCondition", $"timeInSession" >= 60 && $"timeInSession" < 60 * 5)
      .withColumn("thirdCondition", $"timeInSession" >= 60 * 5)
      .groupBy("sessionId")
      .agg(
        countDistinct($"userId", when($"firstCondition", true)).alias("less1"),
        countDistinct($"userId", when($"secondCondition", true)).alias("1to5"),
        countDistinct($"userId", when($"thirdCondition", true)).alias("more5"),
        collect_list($"timeInSession").as("timeInSession")
      )
      .map { row =>
        val sortedWindow = row.getAs[mutable.WrappedArray[Long]]("timeInSession").sorted.toList
        val windowSize = sortedWindow.size
        val m = if (windowSize % 2 == 0) (sortedWindow(windowSize / 2) + sortedWindow(windowSize / 2 - 1)) / 2
        else sortedWindow((windowSize + 1) / 2 - 1)
        (row.getString(0), row.getLong(1), row.getLong(2), row.getLong(3), m)
    }
    .toDF("sessionId", "less1", "1to5", "more5", "median")
  }

  private def top10ProductsByCategory(df: DataFrame): DataFrame = {
    import sparkSession.implicits._

    df
      .withColumn("timeInSessionByUser", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))
      .select('category, 'product,
        sum('timeInSessionByUser).over(Window.partitionBy('category, 'product).orderBy('timeInSessionByUser.desc)).as("timeInSessionByProduct")
      )
      .withColumn("rn", row_number().over(Window.partitionBy('category).orderBy('timeInSessionByProduct.desc)))
      .where('rn <= 2)
      .drop('rn)
      .drop('timeInSessionByProduct)
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
    import WindowFunctionSession._
    sessionizeByProduct(df)
      .transform(top10ProductsByCategory)
  }

  def dummyWriter()(df: DataFrame): Unit = df.collect() foreach println

}
