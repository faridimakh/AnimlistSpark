package configs

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

trait final_configs {
  //load configuration path infos:
  private final lazy val myconf: Config = ConfigFactory.load()
  final val data_csv_path: String = myconf.getString("source_animlist.input.data_path")
  final val queries_path: String = myconf.getString("source_animlist.input.queries_path")
  final val queries_result_path: String = myconf.getString("source_animlist.output.queries_result_path")
  final val saprkkname: String = myconf.getString("spark.name")
  final val sparkmaster: String = myconf.getString("spark.master")
  final val logstop: String = myconf.getString("spark.stop_log")
  //tables view names--------------------------------------------------------------------------------------------------------------
  final val vu1 = "UserAnimeList_view"
  final val vu2 = "AnimeList_view"
  final val vu3 = "UserList_view"
  //tables view names--------------------------------------------------------------------------------------------------------------
  final val datas_csv_name = Array("AnimeList.csv", "UserList.csv", "UserAnimeList.csv")
  final val tun_test_request: Seq[(String, String)] = Seq(("vu1", vu1), ("vu2", vu2), ("vu3", vu3))

  //Declare schema for related column---------------------------------------------------------------------------------------------------------------------------------------------------------
  final val internalShemaRelated: StructType = new StructType()
    .add("mal_id", IntegerType)
    .add("type", StringType)
    .add("url", StringType)
    .add("title", StringType)
  final var related_shema: StructType = new StructType()
  Array("Adaptation", "Sequel", "Summary", "Prequel", "Alternativeversion", "Alternativesetting", "Other")
    .map(x => related_shema = related_shema.add(x, internalShemaRelated, nullable = true))

  //Declare schema for aired column---------------------------------------------------------------------------------------------------------------------------------------------------------
  final val aired_schema: StructType = new StructType()
    .add("from", DateType)
    .add("to", DateType)

}
