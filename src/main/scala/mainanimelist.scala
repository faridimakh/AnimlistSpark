import tools.fonctions.final_func._
import tools.fonctions.shell_process._
import tools.static_vals.satatic_values.{spark, vu1, vu2, vu3, _}

object mainanimelist {

  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel(myconf.getString("spark.stop_log"))
    if (check_configuration_running == 1) {
      clean_path_query_for_storage_before_next_process

      process_UserAnimeList().createOrReplaceTempView(vu1)
      process_AnimeList().createOrReplaceTempView(vu2)
      process_UserList().createOrReplaceTempView(vu3)
      println("les requettes stockées dans le repertoire \n" + path_queries_to_process + "\n sont lancées sur les données stockées dans \n" + data_path)
      println("...\n  \n...")
      Seq("req2", "req3", "req4", "req5", "req6", "req7", "req8").foreach(x => save_df(spark.sql(PathQuery_to_StringQuery(req_textFormat = x + ".txt")).dropDuplicates(), namedf = x))
      println("les requettes sont calculées correctement, sont stockées dans le reperoire suivant: \n" + path_query_for_storage)
      clean_Queries
      println(clean_Queries)
    }
    else
      println("Erreur! un ou plusieurs champs sont mal renseignés, ...vérifier le fichier: \n[ src/main/resources/application.conf ] ")
  }
}
