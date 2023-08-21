# spark-utilities
Collection of some practical spark ideas

# Articles written by me to explain some of the code in this repository

1. https://www.linkedin.com/pulse/adapting-avro-schemas-generated-spark-support-schema-krishna-murthy/
2. https://www.linkedin.com/pulse/introduction-swift-messages-how-convert-them-files-krishna-murthy/


val lowerCaseXmlRDD = xmlRDD.map(line => line.replaceAll("(?i)<(/?)(\\w+)([^>]*>)", "<$1${2.toLowerCase}$3"))

def matchSchema(df: DataFrame, structCol: String, customSchema: StructType): DataFrame = {
  // Select the columns from the struct column in the order specified by customSchema
  val colsToKeep = customSchema.fieldNames.map(c => col(s"$structCol.$c").alias(c))
  // Create a new struct column with only the desired fields
  val newStructCol = struct(colsToKeep: _*).alias(structCol)
  // Add the new struct column to the DataFrame and drop the old one
  val df2 = df.withColumn(structCol, newStructCol)
  
  // Compare the schema of the field in the DataFrame with the customSchema
  val dfSchema = df2.select(structCol).schema.head.dataType.asInstanceOf[StructType]
  val missingFields = customSchema.filterNot(f => dfSchema.exists(_.name == f.name))
  
  // Add missing fields from customSchema to the DataFrame
  val colsToAdd = missingFields.map(f => lit(null).cast(f.dataType).alias(f.name))
  val finalStructCol = struct((colsToKeep ++ colsToAdd): _*).alias(structCol)
  df2.withColumn(structCol, finalStructCol)
}
