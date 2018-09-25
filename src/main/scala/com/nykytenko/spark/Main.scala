package com.nykytenko

import cats.effect.{Effect, IO}
import cats.implicits._
import com.nykytenko.spark.{EtlDescription, ProcessData, Session}
import org.apache.spark.sql.SparkSession


object Main {
  case class EtlResult(value: EtlDescription, session: SparkSession)

  def main(args: Array[String]): Unit = {

    val name = args.head.toLowerCase.trim
    program[IO](name).unsafeRunSync()
  }

  def program[F[_]](etlName: String)(implicit E: Effect[F]): F[Unit] = {
    for {
      logic <- mainLogic[F](etlName)
      _     <- logic.value.process()
      _     <- Session[F].close(logic.session)
    } yield ()
  }

  def mainLogic[F[_]](name:String)(implicit E: Effect[F]): F[EtlResult] = {
    for {
      configuration <- config.load[F]
      session       <- new Session[F].createFromConfig(configuration.spark)
      processData   = new ProcessData(configuration.csv)(session)
      result        = processData.mapEtl(name)
    } yield EtlResult(result, session)
  }
}
