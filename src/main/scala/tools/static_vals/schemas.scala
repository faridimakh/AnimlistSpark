package tools.static_vals

import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

object schemas {

  //Declare schema for related column---------------------------------------------------------------------------------------------------------------------------------------------------------
  val internalShemaRelated: StructType = new StructType()
    .add("mal_id", IntegerType)
    .add("type", StringType)
    .add("url", StringType)
    .add("title", StringType)
  var related_shema: StructType = new StructType()
  Array("Adaptation", "Sequel", "Summary", "Prequel", "Alternativeversion", "Alternativesetting", "Other")
    .map(x => related_shema = related_shema.add(x, internalShemaRelated, nullable = true))

  //Declare schema for aired column---------------------------------------------------------------------------------------------------------------------------------------------------------
  val aired_schema: StructType = new StructType()
    .add("from", DateType)
    .add("to", DateType)
}
