```scala
def readHeaderRows(filePath: String, numHeaderRows: Int): List[String] = {
  val reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"))
  try {
    (1 to numHeaderRows).map(_ => reader.readLine()).toList
  } finally {
    reader.close()
  }
}

  val headerRows: List[String] = readHeaderRows(csvPath, numHeaderRows)

  // Validate header order
  if (headerRows.size != numHeaderRows || !headerRows.headOption.contains(expectedHeaderOrder)) {
    println("Header validation failed. Actual header does not match the expected order.")
    return false
  }

```
