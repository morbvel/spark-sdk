name := "scalasparsdk.testing"

organization := "com.org.miguel"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.17",
  "org.pegdown" % "pegdown" % "1.6.0",
  "org.slf4j" % "slf4j-reload4j" % "2.0.9",
  "org.scalacheck" %% "scalacheck" % "1.17.0"
)