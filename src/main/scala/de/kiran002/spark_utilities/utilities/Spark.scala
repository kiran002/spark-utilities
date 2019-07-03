package de.kiran002.spark_utilities.utilities

import org.apache.spark.sql.SparkSession

object Spark {
  def createLocalSparkSession(application: String = "Default application"): SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName(application)
      .getOrCreate()
  }
}
