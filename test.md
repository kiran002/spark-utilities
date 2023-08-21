
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
