ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

lazy val root = (project in file("."))
  .settings(
    name := "processors-oss"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.2.1" % "provided",
  "org.postgresql" % "postgresql" % "42.3.6",
)
assemblyJarName in assembly := s"telegram-job-processing.jar"