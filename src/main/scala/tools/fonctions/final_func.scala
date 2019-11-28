package tools.fonctions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import tools.fonctions.udf_func.{date_Detection_udf, detectNumericStarting_udf}
import tools.static_vals.final_values._
import tools.static_vals.schemas.{aired_schema, related_shema}

object final_func {
  def process_AnimeList(): DataFrame = {
    val integer_extractor_rating_column: Column = regexp_extract(col("rating"), """[0-9]+""", 0)
    val related_replacing: Column = regexp_replace(col("related"), "[\\[\\]\\ ]", "")
    //    val related_replacing_info = Map("Alternative version" -> "Alternative_version", "Alternative setting" -> "Alternative_setting")
    val column_type_transform_info = Map[String, String]("anime_id" -> "Int", "members" -> "Int", "episodes" -> "Int", "scored_by" -> "Int", "rank" -> "Int", "favorites" -> "Int", "popularity" -> "double", "score" -> "double")
    val rating_replacing_info = Map("All|Ages|Children|None" -> "0", "Nudity|Hentai" -> "21", "[a-z]" -> "")

    //-------------------------------------------------------------------------------------------------------------------------------
    var df1: DataFrame = AnimeList
    //  0) trim all columns:
    df1.columns.foreach(x => df1 = df1
      .withColumn(x, trim(col(x))).na.fill(0))

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
    // 3) parcer le le champ related:
    df1 = df1.withColumn("related", from_json(related_replacing, related_shema))

    //-------------------------------------------------------------------------------------------------------------------------------
    // 3) parcer le le champ airing:
    df1 = df1.withColumn("airing", trim(col("airing")).cast("boolean")).na.fill(false)

    //-------------------------------------------------------------------------------------------------------------------------------
    //  6)parcer le le champ rating:
    val transforme_to_expression = rating_replacing_info.keys.map(x => regexp_replace(lower(col("rating")), x.toLowerCase, rating_replacing_info(x)))
    transforme_to_expression.foreach(x => df1 = df1.withColumn("rating", x))
    df1 = df1.withColumn("rating", integer_extractor_rating_column.cast("Int"))

    //aired_string is duplicated
    df1 = df1.drop("aired_string")
    df1
  }

  def process_UserAnimeList(): DataFrame = {
    var df = UserAnimeList
    df = df.filter(!detectNumericStarting_udf(col("username")))
    val replacig_unknown_values_my_tags_column = regexp_replace(trim(col("my_tags")), """^[0-9].+""", "Unknown")
    df = df.withColumn("my_tags", replacig_unknown_values_my_tags_column.cast("string"))
      .withColumn("my_tags", split(trim(col("my_tags")), ","))
    df
  }

  def process_UserList(): DataFrame = {
    var df = UserList
    List("location", "username").foreach(x => df = df.withColumn(x, trim(col(x))))
    df = df.filter(date_Detection_udf(col("birth_date")))
      .withColumn("birth_date", to_date(col("birth_date")))
      .withColumn("stats_mean_score", col("stats_mean_score").cast("double"))
      .withColumn("stats_rewatched", col("stats_rewatched").cast("int"))
      .drop("access_rank")
    df
  }

  def save_df(df: DataFrame, nb_partition: Int = 1, format_saving: String = "com.databricks.spark.csv", path: String = path_query_for_storage, namedf: String): Unit = {
    df.coalesce(nb_partition).write.mode(SaveMode.Overwrite).format(format_saving).option("header", "true")
      .save(path + namedf)
  }

  def PathQuery_to_StringQuery(path_req_textFormat: String = path_queries_to_process, req_textFormat: String): String = {
    val req = spark.sparkContext.textFile(path_req_textFormat + req_textFormat).coalesce(1)
    val reqex = req
      .map(x => x
        .replace("  ", " ")
        .replace("vu1", vu1)
        .replace("vu2", vu2)
        .replace("vu3", vu3)
        .trim + " ")
      .reduce(_ + _)
    reqex

  }
}
