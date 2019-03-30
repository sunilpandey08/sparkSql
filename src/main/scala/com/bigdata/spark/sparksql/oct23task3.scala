package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object oct23task3 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("oct23task3").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("oct23task3").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("oct23task3").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.functions._
    val data = "C:\\work\\datasets\\10000Records.csv"
    val df = spark.read.format("csv").option("delimiter",",").option("header","true").option("inferSchema","true").load(data)
    val colms = df.columns.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+", ""))
    val colDF = df.toDF(colms:_*)
    //colDF.show(2)
    val res = colDF.select($"FirstName",$"LastName",$"PhoneNo").withColumn("NewPhone",regexp_replace($"PhoneNo","[^\\p{L}\\p{Nd}]+",""))
    res.show(5)

    spark.stop()
  }
}
