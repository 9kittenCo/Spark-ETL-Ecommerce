package com.nykytenko

import cats.data.Ior
import cats.effect.{Effect, IO}
import cats.implicits._
import com.nykytenko.spark.{EtlDescription, ProcessData, Session}
import org.apache.spark.sql.{Row, SparkSession}


object Main {
  type Result[T] = Option[T]
  case class EtlResult(value: EtlDescription, session: SparkSession)

  def main(args: Array[String]): Unit = {

      //todo as test example output as println to command line
    val name = args.headOption.getOrElse("_1").toLowerCase.trim
    program[IO](name).unsafeRunSync() foreach println
  }

  def program[F[_]](etlName: String)(implicit E: Effect[F]): F[Array[Row]] = {
    for {
      logic       <- mainLogic[F](etlName)
      value       <- logic.value.process()
//      executedETL <-  value
      _           <- Session[F].close(logic.session)
    } yield value
  }

  def mainLogic[F[_]](name:String)(implicit E: Effect[F]): F[EtlResult] = {
    for {
      configuration <- config.load[F]
      session       <- new Session[F].createFromConfig(configuration.spark)
      processData   = new ProcessData(configuration.csv, session)
      result = {
        if (name == "_1") processData.Etls._1
        else if (name == "_2") processData.Etls._2
        else if (name == "_3") processData.Etls._3
        else processData.Etls._1
      }
    } yield EtlResult(result, session)
  }
}
