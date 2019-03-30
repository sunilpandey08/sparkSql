  name := "sparkSql"
  version := "0.1"
  scalaVersion := "2.11.8"
  val sparkVersion = "2.3.1"
  val flinkVersion = "1.6.2"
  val mysqlVersion = "6.0.6"
  val calciteVersion = "1.16.0"
  val kafkaVersion= "0.10.0.0"
  val sparkXml="0.4.1"

  libraryDependencies ++= Seq(
    "org.apache.flink" %% "flink-scala" % flinkVersion,
    "org.apache.flink" % "flink-core" % flinkVersion,
    "org.apache.flink" %% "flink-clients" % flinkVersion,
    "org.apache.flink" % "flink-jdbc" % flinkVersion,
    "mysql" % "mysql-connector-java" % mysqlVersion,
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
    "org.apache.flink" %% "flink-table" % flinkVersion,
    "org.apache.flink" %% "flink-connector-twitter" % flinkVersion,
    // https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka-0.10
    "org.apache.flink" %% "flink-connector-kafka-0.10" %  flinkVersion,
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.kafka" %% "kafka" % kafkaVersion,
    "org.apache.kafka" % "kafka-clients" % kafkaVersion,
    "org.apache.kafka" % "kafka-streams" % kafkaVersion,
    "com.databricks" %% "spark-xml" % sparkXml,
    "com.crealytics" % "spark-excel_2.11" % "0.11.0"
  )
  // https://mvnrepository.com/artifact/com.databricks/spark-xml
  //libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"
  // https://mvnrepository.com/artifact/org.apache.spark/spark-hive
  //libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1" % "provided"
  // https://mvnrepository.com/artifact/com.twitter/jsr166e
  //libraryDependencies += "com.twitter" % "jsr166e" % "1.1.0"
  // https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector

  // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10

  // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka
  //libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"
  // https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
  //libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.0.0"
  // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
  //libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"
  // https://mvnrepository.com/artifact/org.apache.kafka/kafka

  // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
  //libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1" % "provided"


