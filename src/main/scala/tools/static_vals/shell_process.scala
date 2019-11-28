package tools.static_vals
import scala.sys.process._
import tools.static_vals.final_values.{data_path, path_queries_to_process, path_query_for_storage}

object shell_process {
  Process("chmod 777 src/main/resources/script.sh").!
  lazy val check_configuration_running: Int = Seq("./src/main/resources/script.sh", data_path, path_queries_to_process, path_query_for_storage).!!.trim.toInt
  lazy val clean_Queries: Int =Process("find /home/farid/Bureau/requettodellet/ -not -name *.csv -type f -delete").!
}
