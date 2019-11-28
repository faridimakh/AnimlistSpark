import tools.fonctions.final_func._
import tools.static_vals.final_values.{spark, vu1, vu2, vu3, _}

import scala.sys.process.Process

object mainanimelist {

  def main(args: Array[String]): Unit = {

    process_UserAnimeList().createOrReplaceTempView(vu1)
    process_AnimeList().createOrReplaceTempView(vu2)
    process_UserList().createOrReplaceTempView(vu3)

    println("les requettes stockées dans le repertoire \n" + path_queries_to_process + "\n sont lancées sur les données stockées dans \n" + data_path)
    println("...\n  \n...")
    Seq("req2", "req3", "req4", "req5", "req6", "req7", "req8").foreach(x => save_df(spark.sql(PathQuery_to_StringQuery(req_textFormat = x + ".txt")), namedf = x))
    println("les requettes sont calculées correctement, sont stockées dans le reperoire suivant: \n" + path_query_for_storage)
    //netoyage des requettes
    Process("find "+path_query_for_storage+" -not -name *.csv -type f -delete").run()


  }
}
