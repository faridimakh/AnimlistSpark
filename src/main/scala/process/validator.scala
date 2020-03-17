package process

import configs.shared_configs
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

object validator extends Thread with shared_configs {

  private def process_AnimeList(): DataFrame = {
    lazy val integer_extractor_rating_column: Column = regexp_extract(col("rating"), """[0-9]+""", 0)
    lazy val related_replacing: Column = regexp_replace(col("related"), "[\\[\\]\\ ]", "")
    lazy val column_type_transform_info = Map[String, String]("anime_id" -> "Int", "members" -> "Int", "episodes" -> "Int", "scored_by" -> "Int", "rank" -> "Int", "favorites" -> "Int", "popularity" -> "double", "score" -> "double")
    lazy val rating_replacing_info = Map("All|Ages|Children|None" -> "0", "Nudity|Hentai" -> "21", "[a-z]" -> "")

    //-------------------------------------------------------------------------------------------------------------------------------
    var df1: DataFrame = brute_3_datas.head
    //  0) trim all columns:
    df1.columns.foreach(x => df1 = df1.withColumn(x, trim(col(x))).na.fill(0))
    df1 = df1.filter(!col("licensor").contains("}") || col("licensor").isNull)

    //-------------------------------------------------------------------------------------------------------------------------------
    //  1) parcer le le champ "genre","title_synonyms":
    Array("genre", "title_synonyms", "producer").foreach(x => df1 = df1
      .withColumn(x, split(col(x), ",")))

    //-------------------------------------------------------------------------------------------------------------------------------
    //  2) parcer le le champ aired:
    df1 = df1.withColumn("aired", from_json(col("aired"), aired_schema))

    //-------------------------------------------------------------------------------------------------------------------------------
    column_type_transform_info.keys.foreach(x => df1 = df1
      .withColumn(x, col(x).cast(column_type_transform_info(x))).na.fill(0))

    //-------------------------------------------------------------------------------------------------------------------------------
    // 4) parcer le le champ related:
    df1 = df1.withColumn("related", from_json(related_replacing, related_shema))

    //-------------------------------------------------------------------------------------------------------------------------------
    // 5) parcer le le champ airing:
    df1 = df1.withColumn("airing", trim(col("airing")).cast("boolean")).na.fill(false)

    //-------------------------------------------------------------------------------------------------------------------------------
    //  6)parcer le le champ rating:
    lazy val transforme_to_expression = rating_replacing_info.keys.map(x => regexp_replace(lower(col("rating")), x.toLowerCase, rating_replacing_info(x)))
    transforme_to_expression.foreach(x => df1 = df1.withColumn("rating", x))
    df1 = df1.withColumn("rating", integer_extractor_rating_column.cast("Int"))

    //aired_string is duplicated
    df1 = df1.drop("aired_string")
    df1
  }

  private def process_UserAnimeList(): DataFrame = {
    var df = brute_3_datas(2)
    df = df.filter(!detectNumericStarting_udf(col("username")))
    val replacig_unknown_values_my_tags_column = regexp_replace(trim(col("my_tags")), """^[0-9].+""", "Unknown")
    df = df.withColumn("my_tags", replacig_unknown_values_my_tags_column.cast("string"))
      .withColumn("my_tags", split(trim(col("my_tags")), ","))
    df
  }


  private def process_UserList(): DataFrame = {
    var df = brute_3_datas(1)
    List("location", "username").foreach(x => df = df.withColumn(x, trim(col(x))))
    df = df.filter(date_Detection_udf(col("birth_date")))
      .withColumn("birth_date", to_date(col("birth_date")))
      .withColumn("stats_mean_score", col("stats_mean_score").cast("double"))
      .withColumn("stats_rewatched", col("stats_rewatched").cast("int"))
      .drop("access_rank")
    df
  }

  def map_replacing(str: String, text1bytext2: Seq[(String, String)]): String = {
    var strout_put = str //      val p = '"' /*s"$p"*/
    text1bytext2.foreach(x => strout_put = strout_put.replace(x._1, x._2))
    strout_put
  }

  private def PathQuery_to_StringQuery(path_req_textFormat: String = queries_path, req_name_file_text: String): String = {

    val req = spark.sparkContext.textFile(path_req_textFormat.concat("/") + req_name_file_text).coalesce(1)
    val reqex = req
      .map(x => map_replacing(x, tun_test_request).trim + " ")
      .reduce(_ + _)
    reqex
  }

  //detect if column start with numeric value
  private def detectNumericStarting(s: Any): Boolean = if (s == null) true else s.toString.matches("(" + 0.to(9).map(_.toString).mkString("|") + ")" + ".*")
  private lazy val detectNumericStarting_udf: UserDefinedFunction = udf(detectNumericStarting(_: String): Boolean)

  //date detection:
  private def date_Detection(str: Any): Boolean = {if (str == null) true else str.toString.matches("\\d+.\\d+.\\d+")}
  private lazy val date_Detection_udf: UserDefinedFunction = udf(date_Detection(_: String): Boolean)

  private def save_df(df: DataFrame,
                      nb_partition: Int = 1,
                      format_saving: String = "com.databricks.spark.csv",
                      path: String = queries_result_path,
                      namedf: String): Unit = {
    df.coalesce(nb_partition).write.mode(SaveMode.Overwrite).format(format_saving).option("header", "true")
      .save(path.concat("/") + namedf)
  }


  //------------------------------------------------------------------------------------------------------------------------------------------------
  //to process what you want here:
  //you can delete or put other hql queries example for only req2 and req3 do:
  // private val quries_file_names = Seq("req2", "req3")
  //------------------------------------------------------------------------------------------------------------------------------------------------

  private val quries_file_names = Seq("req2", "req3", "req4", "req5", "req6", "req7", "req8")
  //load  csv Data----------------------------------------------------------------------------------------------------------------------------------
  private lazy val brute_3_datas: Array[DataFrame] = datas_csv_name.map(data_csv_path.concat("/") + _).map(x => spark_csv_reader.load(x))

  override def run(): Unit = {
    //clean  csv loaded-------------------------------------
    process_UserAnimeList().createOrReplaceTempView(vu1)
    process_AnimeList().createOrReplaceTempView(vu2)
    process_UserList().createOrReplaceTempView(vu3)
    //process quries-----------------------------------------
    quries_file_names.foreach(x => save_df(spark.sql(PathQuery_to_StringQuery(req_name_file_text = x + ".txt"))
      .dropDuplicates(), namedf = x))
  }
}
