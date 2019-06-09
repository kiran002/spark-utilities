package de.kiran002.spark_utilities.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import de.kiran002.spark_utilities.avro.schema.{SchemaConverters => CustomConverters}

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val listOfStudents = Seq(Student(1, "One"), Student(2, "Two"), Student(3, "Three"), Student(4, "Four"))
    import spark.implicits._

    val df = listOfStudents.toDF()

    df.printSchema()

    println(SchemaConverters.toAvroType(df.schema))
    println(CustomConverters.toAvroType(df.schema))
  }
}

case class Student(id: Int, name: String)