```Scala
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{StructType, StructField, ArrayType}

def matchSchema(df: DataFrame, structCol: String, customSchema: StructType): DataFrame = {
  def matchStructType(df: DataFrame, structCol: String, customSchema: StructType): DataFrame = {
    // Select the columns from the struct column in the order specified by customSchema
    val colsToKeep = customSchema.fields.map { f =>
      val colName = s"$structCol.${f.name}"
      f.dataType match {
        case st: StructType => struct(matchStructType(df.select(colName), colName, st).columns.map(col): _*).alias(f.name)
        case ArrayType(st: StructType, _) => col(colName).alias(f.name)
        case _ => col(colName).alias(f.name)
      }
    }
    // Create a new struct column with only the desired fields
    val newStructCol = struct(colsToKeep: _*).alias(structCol)
    // Add the new struct column to the DataFrame and drop the old one
    df.withColumn(structCol, newStructCol)
  }

  // Match the top-level schema of the struct column with customSchema
  val df2 = matchStructType(df, structCol, customSchema)

  // Compare the schema of the field in the DataFrame with customSchema
  val dfSchema = df2.select(structCol).schema.head.dataType.asInstanceOf[StructType]
  val missingFields = customSchema.filterNot(f => dfSchema.exists(_.name == f.name))

  // Add missing fields from customSchema to the DataFrame
  val colsToAdd = missingFields.map(f => lit(null).cast(f.dataType).alias(f.name))
  val finalStructCol = struct((dfSchema.fields.map(f => col(s"$structCol.${f.name}").alias(f.name)) ++ colsToAdd): _*).alias(structCol)
  df2.withColumn(structCol, finalStructCol)
}

// Example usage
val data = Seq((1, (("a", Array(("x", "y"))), 1)), (2, (("b", Array(("x", "y"))), 2)), (3, (("c", Array(("x", "y"))), 3)))
val df = spark.createDataFrame(data).toDF("id", "nested")
df.show()
// +---+----------------+
// | id|          nested|
// +---+----------------+
// |  1|[[a,[[x,y]]],1]|
// |  2|[[b,[[x,y]]],2]|
// |  3|[[c,[[x,y]]],3]|
// +---+----------------+

val customSchema = StructType(Seq(StructField("a", StructType.fromDDL("b array<struct<c:string>>")), StructField("b", StructType.fromDDL("c string"))))
val df2 = matchSchema(df, "nested", customSchema)
df2.show()
// +---+------------+
// | id|     nested |
// +---+------------+
// | 1 |[[[null]],null]|
// | 2 |[[[null]],null]|
// | 3 |[[[null]],null]|
// +---+------------+


```


```
script:
files=("$baseLocation/$currentDate/$projectLocation/$filePattern")
    if [ ${#files[@]} -gt 0 ]; then

```


```
import scala.xml._

object Main extends App {
  val xml = "<Tag attribute=\"value\">Content<InnerTag>More Content</InnerTag></Tag>"
  val parsedXml = XML.loadString(xml)
  val transformedXml = new RuleTransformer(new RewriteRule {
    override def transform(node: Node): Seq[Node] = node match {
      case e: Elem => e.copy(label = e.label.toLowerCase, attributes = e.attributes.map(a => a.copy(key = a.key.toLowerCase)))
      case other => other
    }
  }).transform(parsedXml)
  println(transformedXml)
}
```
