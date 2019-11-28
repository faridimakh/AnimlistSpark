package tools.fonctions

import tools.static_vals.satatic_values.{data_path, path_queries_to_process, path_query_for_storage}

import scala.sys.process._

object shell_process {
  Process("chmod 777 src/main/resources/script.sh").!
  lazy val check_configuration_running: Int = Seq("./src/main/resources/script.sh", data_path, path_queries_to_process, path_query_for_storage).!!.trim.toInt
  lazy val clean_Queries: Int = Seq("find" ,path_query_for_storage, "-not","-name","*.csv", "-type","f","-delete").!
  lazy val clean_path_query_for_storage_before_next_process: Int = Seq("find" ,path_query_for_storage, "-name","*.csv", "-type","f","-delete").!
}