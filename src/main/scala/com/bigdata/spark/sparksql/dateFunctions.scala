package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object dateFunctions {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("dateFunctions").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("dateFunctions").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("dateFunctions").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val data = "C:\\work\\datasets\\mysqldata.csv"
    val empDf = spark.read.format("csv").option("delimiter",",").option("header","true").option("dateFormat","yyyy-MM-dd").option("inferSchema","true").load(data)
    val partitionWindow = Window.partitionBy($"deptno").orderBy($"sal".desc)
    val rankTest = rank().over(partitionWindow)
    empDf.select($"*", rankTest as "rank").filter($"rank"===3).show

    spark.stop()
  }
}
