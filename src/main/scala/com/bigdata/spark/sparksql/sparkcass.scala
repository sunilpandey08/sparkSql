package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

object sparkcass {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkcass").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkcass").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkcass").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
      //to read the data from cassandra
    val df = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","cassdb").option("table","asl").load()



    spark.stop()
  }
}
