package toolkits

object mainclass extends satatic_values  {
  def main(args: Array[String]): Unit = {
    if (check_configuration_running) {
      cleaner.run()
      println(info_running)
      final_func.run()
      println(success_running)
    }
    else {
      println(logger)
    }
    close_spark
  }
}
/*
--pakchege  application:
sbt clean assembly
--lunch the jar
spark-submit  AnimlistSpark-assembly-0.1.jar*/
