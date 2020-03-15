package toolkits

import org.apache.hadoop.fs.Path

import scala.sys.process
import scala.sys.process.Process

trait satatic_values extends sharedObjects {
  lazy val check_configuration_running: Boolean = count_hdfs_existing_path(List(data_csv_path, queries_path, queries_result_path)).equals(3)
  lazy val cleaner: process.ProcessBuilder = Process(fs.delete(new Path(queries_result_path), true))

  //logger--------------------------------------------------------------------------------------------------------------
  lazy val logger: String = to_hdfs_paths(List(data_csv_path, queries_path, queries_result_path)).filter(fs.exists(_) == false)
    .head.toString + " !!! path not found! give a valid path in \n src/main/resources/application.conf "

  //success info--------------------------------------------------------------------------------------------------------------
  lazy val info_running : String = "queries stored in the directory \n" + queries_path +
    "\n are lunched for processing data located in " + "\n" + data_csv_path + "...\n  \n..."
  lazy val  success_running: String = "success! get your results in the following repository: \n" + queries_result_path




}
