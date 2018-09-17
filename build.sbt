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