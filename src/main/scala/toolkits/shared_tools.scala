package toolkits

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrameReader, SparkSession}

trait shared_tools {
  //load configuration path infos:
  private final lazy val myconf: Config = ConfigFactory.load()
  final lazy val data_csv_path: String = myconf.getString("source_animlist.input.data_path")
  final lazy val queries_path: String = myconf.getString("source_animlist.input.queries_path")
  final lazy val queries_result_path: String = myconf.getString("source_animlist.output.queries_result_path")
  //get a sparak session:

   lazy val spark: SparkSession = new SparkSession.Builder()
    .appName(myconf.getString("spark.name"))
    .master(myconf.getString("spark.master"))
    .config("spark.debug.maxToStringFields", 100)
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .getOrCreate()
  lazy val close_spark: Unit = spark.stop()

  spark.sparkContext.setLogLevel(myconf.getString("spark.stop_log"))
  //tables view names--------------------------------------------------------------------------------------------------------------
  final lazy val vu1 = "UserAnimeList_view"
  final lazy val vu2 = "AnimeList_view"
  final lazy val vu3 = "UserList_view"

  //tables view names--------------------------------------------------------------------------------------------------------------
  final lazy val datas_csv_name = Array("AnimeList.csv", "UserList.csv", "UserAnimeList.csv")

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

  lazy val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  //get a loader csv:---------------------------------------------------------------------------------------------------------------------
  lazy val spark_csv_reader: DataFrameReader = spark.read.format("""csv""").option("header", "true").option("inferSchema", "true")

  //chered methods:---------------------------------------------------------------------------------------------------------------------
  def to_hdfs_paths(s: Seq[String]): Seq[Path] = s.map(x => new Path(x))

  def count_hdfs_existing_path(listStringtoPath: List[String]): Int = {
    to_hdfs_paths(listStringtoPath).map(x => if (fs.exists(x)) 1 else 0).sum
  }


}
