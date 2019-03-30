package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object oct26task {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("oct26task").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("oct26task").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("oct26task").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //oracle database connection
    val orUrl = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"

    val orProp = new java.util.Properties()
    orProp.setProperty("user","ousername")
    orProp.setProperty("password","opassword")
    orProp.setProperty("driver","oracle.jdbc.OracleDriver")
    val query = s"(select * from AIRLINEDATA) AIRLINEDATA"
    val oDf = spark.read.jdbc(orUrl,query ,orProp)
    oDf.take(10)
    //oDf.createOrReplaceTempView("airline")
    //val oraData = spark.sql("select * from airline limit 100")
    //oraData.show()

    spark.stop()
  }
}
