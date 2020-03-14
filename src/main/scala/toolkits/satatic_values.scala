package toolkits

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import toolkits.final_func.{count_hdfs_existing_path, to_hdfs_paths}

import scala.sys.process
import scala.sys.process.Process

object satatic_values {
  lazy val check_configuration_running: Boolean = count_hdfs_existing_path(List(data_csv_path, queries_path, queries_result_path)).equals(3)
  lazy val cleaner: process.ProcessBuilder = Process(fs.delete(new Path(queries_result_path), true))

  //get a loader csv:---------------------------------------------------------------------------------------------------------------------
  private lazy val spark_csv_reader: DataFrameReader = spark.read.format("""csv""").option("header", "true").option("inferSchema", "true")
  //load  Data
  lazy val brute_3_datas: Array[DataFrame] = Array("AnimeList.csv", "UserList.csv", "UserAnimeList.csv").map(data_csv_path.concat("/") + _)
    .map(x => spark_csv_reader.load(x))

  lazy val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  //logger--------------------------------------------------------------------------------------------------------------
  lazy val logger: String = to_hdfs_paths(List(data_csv_path, queries_path, queries_result_path)).filter(fs.exists(_) == false)
    .head.toString + " !!! path not found! give a valid path in \n src/main/resources/application.conf "

}
