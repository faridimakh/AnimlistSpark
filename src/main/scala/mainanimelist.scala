import toolkits._
import toolkits.final_func.run_and_stock_my_quries
import toolkits.satatic_values.{check_configuration_running, cleaner, logger}

object mainanimelist {
  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel(myconf.getString("spark.stop_log"))
    if (check_configuration_running) {
      cleaner.run()
      println("queries stored in the directory \n" + queries_path +
        "\n are lunched for processing data located in " + "\n" + data_csv_path + "...\n  \n...")
      run_and_stock_my_quries()
      println("success! get your results in the following repository: \n"
        + queries_result_path)
    }
    else
      println(logger)
  }
}
