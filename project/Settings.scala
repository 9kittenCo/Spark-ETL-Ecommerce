import sbt.Keys._

object Settings {
  val common = Seq(
    name         := "Example-Spark-ETL-for-ecommerce",
    organization := "com.9kittenCo",
    version      := "0.0.1-SNAPSHOT",
    scalaVersion := "2.11.12"
  )
}
