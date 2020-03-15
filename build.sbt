name := "AnimlistSpark"

version := "0.1"

scalaVersion := "2.11.12"

val spark_version = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark_version,
  "org.apache.spark" %% "spark-sql" % spark_version,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.typesafe" % "config" % "1.3.1")
mainClass in assembly := Some("toolkits.mainclass")
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}