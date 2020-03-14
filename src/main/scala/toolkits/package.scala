import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

package object toolkits {
  //load configuration path infos:
  final lazy val myconf: Config = ConfigFactory.load()
  final lazy val data_csv_path: String = myconf.getString("source_animlist.input.data_path")
  final lazy val queries_path: String = myconf.getString("source_animlist.input.queries_path")
  final lazy val queries_result_path: String = myconf.getString("source_animlist.output.queries_result_path")
  //get a sparak session:

  final lazy val spark: SparkSession = new SparkSession.Builder()
    .appName(myconf.getString("spark.name"))
    .master(myconf.getString("spark.master"))
    .config("spark.debug.maxToStringFields", 100)
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .getOrCreate()

  //tables view names--------------------------------------------------------------------------------------------------------------
  final lazy val vu1 = "UserAnimeList_view"
  final lazy val vu2 = "AnimeList_view"
  final lazy val vu3 = "UserList_view"

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
