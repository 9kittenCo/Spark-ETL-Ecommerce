
lazy val root = (project in file("."))
  .settings(
    scalacOptions ++= Options.defaults,
    resolvers ++= Resolvers.root,
    libraryDependencies ++= Dependencies.baseDependencies,
    Settings.common
  )

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.configuration=log4j.properties")

mainClass in assembly := Some("com.nykytenko.Main")

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
  case PathList("com", "google", xs @ _*)           => MergeStrategy.last
  case "plugin.properties"          => MergeStrategy.last
  case "git.properties"             => MergeStrategy.last
  case "log4j.properties"           => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName in assembly := s"${name.value}-${scalaVersion.value}-${version.value}.jar"