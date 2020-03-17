package process

import configs.shared_configs
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.sys.process
import scala.sys.process.Process

trait checker extends shared_configs {
  private lazy val fs: FileSystem = FileSystem.get(sparkcontexthadoop)
  lazy val check_configuration_running: Boolean = count_hdfs_existing_path(List(data_csv_path, queries_path, queries_result_path)).equals(3)
  lazy val cleaner: process.ProcessBuilder = Process(fs.delete(new Path(queries_result_path), true))

  //logger--------------------------------------------------------------------------------------------------------------
  lazy val config_exception: String = List(data_csv_path, queries_path, queries_result_path).map(x => new Path(x)).filter(fs.exists(_) == false)
    .head.toString + " !!! path not found! give a valid path in \n src/main/resources/application.conf "

  //success info--------------------------------------------------------------------------------------------------------------
  lazy val info_running: String = "queries stored in the directory \n" + queries_path +
    "\n are lunched for processing data located in " + "\n" + data_csv_path + "...\n  \n..."
  lazy val success_running: String = "success! get your results in the following repository: \n" + queries_result_path

  //chered methods:---------------------------------------------------------------------------------------------------------------------
  private def count_hdfs_existing_path(listStringtoPath: List[String]): Int = {
    listStringtoPath.map(x => new Path(x)).map(x => if (fs.exists(x)) 1 else 0).sum

  }
}