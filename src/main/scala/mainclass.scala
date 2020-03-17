import process.{checker, validator}

object mainclass extends checker {
  def main(args: Array[String]): Unit = {
    if (check_configuration_running) {
      cleaner.run()
      println(info_running)
      validator.run()
      println(success_running)
    }
    else {
      println(config_exception)
    }
    close_spark
  }
}
