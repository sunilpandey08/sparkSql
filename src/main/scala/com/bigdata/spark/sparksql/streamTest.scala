//package com.bigdata.spark.sparksql
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
//import org.apache.spark.sql._
//import org.apache.spark._
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._
//
//object streamTest {
//  def main(args: Array[String]) {
//    //val spark = SparkSession.builder.master("local[*]").appName("streamTest").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//    val ssc = new StreamingContext(conf, Seconds(20))
//    // Create a DStream that will connect to hostname:port, like localhost:9999
//    val lines = ssc.socketTextStream("ec2-13-232-1-233.ap-south-1.compute.amazonaws.com", 9999)
//    val words = lines.flatMap(_.split(" "))
//    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//    wordCounts.print()
//    ssc.start()             // Start the computation
//    ssc.awaitTermination()
//    //spark.stop()
//  }
//}
