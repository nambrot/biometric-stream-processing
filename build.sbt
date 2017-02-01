
name := "biometric-stream-processing"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0"
)

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.4",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.4",
  "com.typesafe.akka" %% "akka-stream" % "2.4.16"
)