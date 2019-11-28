package tools.fonctions

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object udf_func {
  //detect if column start with numeric value
  def detectNumericStarting(s: Any): Boolean = if (s == null) true else s.toString.matches("(" + 0.to(9).map(_.toString).mkString("|") + ")" + ".*")

  val detectNumericStarting_udf: UserDefinedFunction = udf(detectNumericStarting(_: String): Boolean)

  //date detection:
  def date_Detection(str: Any): Boolean = {
    if (str == null) true else str.toString.matches("\\d+.\\d+.\\d+")
  }

  val date_Detection_udf: UserDefinedFunction = udf(date_Detection(_: String): Boolean)
}
