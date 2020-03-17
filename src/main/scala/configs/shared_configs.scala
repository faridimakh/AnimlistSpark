package configs

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrameReader, SparkSession}

trait shared_configs extends final_configs {
  //get a sparak session:
  lazy val spark: SparkSession = new SparkSession.Builder().appName(saprkkname).master(sparkmaster)
    .config("spark.debug.maxToStringFields", "100").config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .getOrCreate()
  //
  //get closer from SparkSession:---------------------------------------------------------------------------------------------------------------------
  lazy val close_spark: Unit = spark.stop()

  //get hadoopConfiguration from SparkSession:---------------------------------------------------------------------------------------------------------------------
  lazy val sparkcontexthadoop: Configuration = spark.sparkContext.hadoopConfiguration

  //get set stop log from SparkSession:---------------------------------------------------------------------------------------------------------------------
  spark.sparkContext.setLogLevel(logstop)

  //get a loader csv from SparkSession:---------------------------------------------------------------------------------------------------------------------
  lazy val spark_csv_reader: DataFrameReader = spark.read.format("""csv""").option("header", "true").option("inferSchema", "true")

}
