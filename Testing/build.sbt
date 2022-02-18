name := "scalasparsdk.testing"

organization := "org.gft"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.9",
  "org.pegdown" % "pegdown" % "1.6.0",
  "org.slf4j" % "slf4j-log4j12" % "1.7.32",
  "org.scalacheck" %% "scalacheck" % "1.15.2"
)