import sbt._

object Dependencies {
  object Version {
    val PureConfig = "0.9.1"
    val Cats       = "1.0.1"
    val CatsEffect = "0.10"
//    val Fs2        = "0.10.4"

    val Spark = "2.3.1"
  }

  val baseDependencies = Seq(
    "com.github.pureconfig" %% "pureconfig"  % Version.PureConfig,

    "org.typelevel"         %% "cats-effect" % Version.CatsEffect,
//    "co.fs2"                %% "fs2-core"    % Version.Fs2,

    "org.apache.spark"      %% "spark-core"  % Version.Spark,
    "org.apache.spark"      %% "spark-sql"   % Version.Spark
  )
}
