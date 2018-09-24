
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
  case PathList("org","aopalliance", xs @ _*)       => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*)        => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*)         => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*)     => MergeStrategy.last
  case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
  case PathList("com", "google", xs @ _*)           => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*)         => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*)           => MergeStrategy.last
  case "about.html"                 => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA"      => MergeStrategy.last
  case "META-INF/mailcap"           => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties"          => MergeStrategy.last
  case "git.properties"             => MergeStrategy.last
  case "log4j.properties"           => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName in assembly := s"${name.value}-${scalaVersion.value}-${version.value}.jar"