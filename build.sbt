import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

name := "ScalaSparkSDK"

organization     := "org.gft"
organizationName := "GFT"

version := (version in ThisBuild).value
scalaVersion := "2.11.12"

scalacOptions += "-target:jvm-1.8"
updateOptions := updateOptions.value.withCachedResolution(true)

val sparkVersion = "2.4.8"

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.scalanlp" %% "breeze" % "1.0",
    "io.delta" %% "delta-core" % "0.6.1",
    "org.scalatest" %% "scalatest" % "3.2.9",
    "org.scalacheck" %% "scalacheck" % "1.15.2"
  ),

  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  assemblyOption in assembly ~= {
    _.copy(includeScala = false)
  },

  test in assembly := {},
  parallelExecution in Test := false,
  publishArtifact in Test := false,
  testOptions in Test += Tests.Argument("-oD"),

  // code coverage settings
  coverageMinimum := 75,
  coverageFailOnMinimum := true,
  coverageEnabled in Test := true,
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

publishTo in ThisBuild := Some(
  ArtifactRegistryResolver.forRepository("https://europe-west2-maven.pkg.dev/engineering-assets-319815/maven-repo")
)