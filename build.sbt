name := "AnimlistSpark"

version := "0.1"

scalaVersion := "2.11.12"

val spark_version = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark_version,
  "org.apache.spark" %% "spark-sql" % spark_version)
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
