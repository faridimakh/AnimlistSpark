package tools.static_vals

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.sys.process._

object final_values {
  //load configuration path infos:
  final val myconf: Config = ConfigFactory.load()

  //get a sparak session:
  final val spark: SparkSession = new SparkSession.Builder()
    .appName(myconf.getString("spark.name"))
    .master(myconf.getString("spark.master")).getOrCreate()

  object spark_reader {
    def type_file(mytype: String = """csv"""): DataFrameReader = {
      spark.read.format(mytype).option("header", "true").option("inferSchema", "true")
    }
  }

  spark.sparkContext.setLogLevel(myconf.getString("spark.stop_log"))
//  spark.conf.set("spark.sql.debug.maxToStringFields", 100)

  //get paths csv files for  data/path queries and path storage queries results
  lazy val data_path: String =myconf.getString("source_animlist.input.data_path")
  lazy val storage_data_cleaned: String = myconf.getString("source_animlist.output.storage_data_cleaned")

   lazy val path_queries_to_process: String = myconf.getString("source_animlist.input.path_queries_to_process")
   lazy val path_query_for_storage: String = myconf.getString("source_animlist.output.path_query_for_storage")



  lazy val list_data_paths: Array[String] = Array("AnimeList.csv", "UserList.csv", "UserAnimeList.csv").map(data_path + _)

  //load  Data:--------------------------------------------------------------------------------------------------------------------------------------
  lazy val AnimeList: DataFrame = spark_reader.type_file().load(list_data_paths.head)
  lazy val UserAnimeList: DataFrame = spark_reader.type_file().load(list_data_paths(2))
  lazy val UserList: DataFrame = spark_reader.type_file().load(list_data_paths(1))
  //tables view tables--------------------------------------------------------------------------------------------------------------
  lazy val vu1 = "UserAnimeList_view"
  lazy val vu2 = "AnimeList_view"
  lazy val vu3 = "UserList_view"
}
