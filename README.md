# spark-utilities
Collection of some practical spark ideas

# Articles written by me to explain some of the code in this repository

1. https://www.linkedin.com/pulse/adapting-avro-schemas-generated-spark-support-schema-krishna-murthy/
2. https://www.linkedin.com/pulse/introduction-swift-messages-how-convert-them-files-krishna-murthy/


val lowerCaseXmlRDD = xmlRDD.map(line => line.replaceAll("(?i)<(/?)(\\w+)([^>]*>)", "<$1${2.toLowerCase}$3"))
