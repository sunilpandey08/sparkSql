package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._


object oct25task {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("oct25task").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("oct25task").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("oct25task").setMaster("local[*]")
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
    //task1) who is the top 10 oldest persons (based on date of birth)
    val res1 = colDF.select($"EmpID",$"FirstName",$"LastName",$"DateofBirth",date_format(to_date(col("DateofBirth"), "MM/dd/yy"), "yyyy-MM-dd").alias("DOB")).orderBy(datediff(current_date(),$"dob") desc).limit(10)
    res1.show()
    //task 2) Who is working for a long time? (from date of joining to today time)
    val res2 = colDF.select($"EmpID",$"FirstName",$"LastName",$"DateofJoining",date_format(to_date(col("DateofJoining"), "MM/dd/yy"), "yyyy-MM-dd").alias("DOJ")).orderBy(datediff(current_date(),$"DOJ") desc).limit(10)
    res2.show(50)

    spark.stop()
  }
}
