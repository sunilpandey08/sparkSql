package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparkUDF {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkUDF").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkUDF").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkUDF").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.functions._
    val data = "C:\\work\\datasets\\bank-full.csv"
    val df = spark.read.format("csv").option("delimiter",";").option("header","true").option("inferSchema","true").load(data)
    //dsl command "concat_ws" function
    df.select(concat_ws(" ",$"job",$"marital").alias("combine")).show()
    //to add the column name in existing columns list with default values
    df.withColumn("criminalrecords",lit("norecords")).show(5)
    // with function is similar to case when in sql
    df.withColumn("agegrp", when($"age".gt(60),("oldage")).when($"age".lt(60),("adult")).otherwise($"age")).show()
    //regexp_extract is to find the matching string in column then
    df.select($"age",$"job").withColumn("suspectacc",regexp_extract($"job","unknown",0)).show()
    df.select($"age",$"job").withColumn("suspectacc",regexp_replace($"job","unknown","***")).show()
    df.withColumn("doy",lpad($"age",4,"19")).withColumn("rpadex",rpad($"doy",10,"0")).show()


    spark.stop()
  }
}
