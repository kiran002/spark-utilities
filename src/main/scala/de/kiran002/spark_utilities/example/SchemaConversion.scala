package de.kiran002.spark_utilities.example

import de.kiran002.spark_utilities.utilities.Spark.createLocalSparkSession
import org.apache.spark.sql.avro.SchemaConverters
import de.kiran002.spark_utilities.avro.schema.{SchemaConverters => CustomConverters}

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val spark = createLocalSparkSession("Schema conversion")

    val listOfStudents = Seq(Student(1, "One"), Student(2, "Two"), Student(3, "Three"), Student(4, "Four"))
    import spark.implicits._

    val df = listOfStudents.toDF()

    df.printSchema()

    println(SchemaConverters.toAvroType(df.schema))
    println(CustomConverters.toAvroType(df.schema))
  }


}

case class Student(id: Int, name: String)