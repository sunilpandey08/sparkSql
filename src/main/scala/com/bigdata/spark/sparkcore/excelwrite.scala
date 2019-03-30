package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql._
import com.crealytics.spark.excel._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.zuinnote.spark.office.excel


object excelwrite {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("excelwrite").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("excelwrite").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("excelwrite").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val iTranSchema = StructType(Array(StructField("name", StringType, true),StructField("age", StringType, true),StructField("city", StringType, true)))
    val iTransNotifSRC_DF = spark.read.option("header","true").option("delimiter",",").schema(iTranSchema).csv("C:\\work\\datasets\\asl3.csv")
    iTransNotifSRC_DF.show()
    //iTransNotifSRC_DF.write.format("com.crealytics.spark.excel").option("dataAddress", "A2:E7").option("useHeader", "true").option("dateFormat","yy-mmm-d").option("timestampFormat", "mm-dd-yyyy hh:mm:ss").mode("append").save("C:\\work\\datasetsWorktime2.xlsx")
    //iTransNotifSRC_DF.write.mode(SaveMode.Overwrite).format("org.zuinnote.spark.office.excel").save("c:\\temp\\test2.xlsx");
    spark.stop()
  }
}
