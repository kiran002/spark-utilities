package de.kiran002.spark_utilities.example

import de.kiran002.spark_utilities.utilities.Spark.createLocalSparkSession
import org.apache.spark.sql.avro.SchemaConverters

import scala.collection.mutable.ListBuffer

object SwiftToAvroConversion extends App {
  val spark = createLocalSparkSession("Swift to avro conversion")

  import com.prowidesoftware.swift.io.RJEReader
  import com.prowidesoftware.swift.model.mt.mt1xx.MT103
  import com.prowidesoftware.swift.utils.Lib

  //initialize the reader
  val reader = new RJEReader(Lib.readResource("swift/mt103.rje", null))

  // create list buffer to hold the list of json strings
  val jsonList = ListBuffer[String]()

  while (reader.hasNext) {
    val msg = reader.nextMT
    if (msg.isType(103)) {
      val mt = msg.asInstanceOf[MT103]
      // convert to json and append to list
      jsonList += mt.toJson
    }
  }

  import spark.implicits._

  // create spark dataframe from the list of json's
  val df = spark.read.json(jsonList.toDS)

  df.show(10)
  df.printSchema()

  // convert structure from df.schema to avro schema
  println(SchemaConverters.toAvroType(df.schema))

}
