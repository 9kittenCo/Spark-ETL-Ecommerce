package com.nykytenko.spark

import cats.effect.{Effect, IO}
import cats.implicits._
import org.apache.spark.sql.SparkSession


case class EtlResult(value: Array[ResultingSet], session: SparkSession)

object Main extends App {

  //todo as test example output as println to command line
  program[IO].unsafeRunSync().map(ResultingSet.unapply(_).get) foreach println

  def program[F[_]](implicit E: Effect[F]): F[Array[ResultingSet]] =
    for {
      logic <- mainLogic[F]
      _     <- Session[F].close(logic.session)
    } yield logic.value

  def mainLogic[F[_]](implicit E: Effect[F]): F[EtlResult] = {
    for {
      configuration <- config.load[F]
      session       <- new Session[F].createFromConfig(configuration.spark)
      resultETL     <- new ProcessData[F](configuration.csv, session).etl1
    } yield EtlResult(resultETL.process(), session)
  }
}
