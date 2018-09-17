package com.nykytenko.spark

import cats.effect.Effect
import pureconfig.error.ConfigReaderException
import cats.implicits._

object config {


  case class SparkConfig(name: String, master: String)

  case class CsvConfig(options: Map[String, String], path: String)

  case class Config(spark: SparkConfig, csv: CsvConfig)

    import pureconfig._

    def load[F[_]](implicit E: Effect[F]): F[Config] = E.delay{
      loadConfig[Config]
    }.flatMap {
        case Right(config) => E.pure(config)
        case Left(e)       => E.raiseError(new ConfigReaderException[Config](e))
      }
 }
