val scala3Version = "3.2.2"
val globalVersion = "0.1.0-SNAPSHOT"

val deps = Seq(
  "org.scalactic" %% "scalactic" % "3.2.15",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "org.scalacheck" %% "scalacheck" % "1.17.0" % "test"
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "a03",
    version := globalVersion,
    scalaVersion := scala3Version,
    libraryDependencies ++= deps
  )
