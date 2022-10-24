ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "Export2Mongo"
  )

val reactivemongoVersion = "1.1.0-noshaded-RC6"
val mongoVersion = "4.7.1"
val json4sVersion = "4.1.0-M1"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % reactivemongoVersion,
  "org.mongodb" % "mongodb-driver-sync" % mongoVersion,
  "org.json4s" %% "json4s-core" % json4sVersion,
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-xml" % json4sVersion,
  "org.scala-lang.modules" %% "scala-xml" % "2.1.0"

  //"io.netty" % "netty-all" % "4.1.81.Final"
)
