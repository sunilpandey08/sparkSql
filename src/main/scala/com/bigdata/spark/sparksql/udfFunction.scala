package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object udfFunction {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("udfFunction").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("udfFunction").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("udfFunction").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.functions._
    val data ="/FileStore/tables/bank_full-bd3df.csv"
    val df = spark.read.format("csv").option("delimiter",";").option("header","true").option("inferSchema","true").load(data)
    df.show()
    df.createOrReplaceTempView("bank")
    def ageoff(a:Int) = {
      if (a<=18) "10% off on books"
      else if (a>18 && a<=30) "20% off on mobiles"
      else if (a>30 && a<=45) "5% off on electricity bills"
      else if (a>45 && a<60) "7% off on mutual funds"
      else if (a>60) "15% on medicins"
      else "plz verify the age"
    }
    //converting method into function
    val ageofffunc = ageoff _
    val agedsludf=udf(ageofffunc) //this function will be used in DSL commands
    spark.udf.register("agesqludf",ageofffunc) //this function will be used in spark sql
    //spark sql way
    val sqlRes = spark.sql("select age,agesqludf(age) offerdesc from bank")
    sqlRes.show()
    // spark DSL way
    val dslRes = df.select("age").withColumn("offerdesc",agedsludf($"age"))
    dslRes.show()

    spark.stop()
  }
}
