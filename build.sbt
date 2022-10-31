ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "Export2Mongo"
  )

val reactivemongoVersion = "1.1.0-noshaded-RC6"
val mongoVersion = "4.7.1"
val jsonOrgVersion = "20220924"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % reactivemongoVersion,
  "org.mongodb" % "mongodb-driver-sync" % mongoVersion,
  "org.json" % "json" % jsonOrgVersion,
)
