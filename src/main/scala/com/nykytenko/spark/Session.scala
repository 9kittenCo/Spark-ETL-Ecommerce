package com.nykytenko.spark

import cats.effect.Effect
import com.nykytenko.spark.config.SparkConfig
import org.apache.spark.sql.SparkSession

case class Session[F[_]](implicit E: Effect[F]) {
  def createFromConfig(config: SparkConfig): F[SparkSession] = E.delay {
    SparkSession.builder
      .master(config.master)
      .appName(config.name)
      .getOrCreate
  }

  def close(sparkSession: SparkSession): F[Unit] = E.delay {
    sparkSession.close()
  }
}
