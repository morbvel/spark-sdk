import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

name := "ScalaSparkSDK"

organization     := "com.org.miguel"
organizationName := "Miguel"

version := (ThisBuild / version).value
scalaVersion := "2.12.18"

scalacOptions += "-target:jvm-1.8"
updateOptions := updateOptions.value.withCachedResolution(true)

val sparkVersion = "3.5.0"

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.scalanlp" %% "breeze" % "2.1.0",
    "io.delta" %% "delta-core" % "2.4.0",
    "org.scalatest" %% "scalatest" % "3.2.17",
    "org.scalacheck" %% "scalacheck" % "1.17.0"
  ),

  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  assembly / assemblyOption ~= {
    _.copy(includeScala = false)
  },

  assembly / test := {},
  Test / parallelExecution := false,
  Test / publishArtifact := false,
  Test / testOptions += Tests.Argument("-oD"),

  // code coverage settings
  coverageMinimum := 75,
  coverageFailOnMinimum := true,
  Test / coverageEnabled := true,
  autoAPIMappings := true
)

lazy val root = Project(id = "ScalaSparkSDK",
  base = file(".")).aggregate(core, testing, validation)
  .settings(commonSettings: _*)

lazy val core = Project(id = "Core",
  base = file("Core")).settings(commonSettings: _*)

lazy val testing = Project(id = "Testing",
  base = file("Testing")).dependsOn(core).settings(commonSettings: _*)

lazy val validation = Project(id = "Validation",
  base = file("Validation")).dependsOn(core, testing).settings(commonSettings: _*)

//publishTo in ThisBuild := Some(
//  ArtifactRegistryResolver.forRepository("maven repo")
//)