package com.bigdata.spark.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

object wordcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("kafka_wordcount").getOrCreate()
    import spark.implicits._
    import spark.sql
    import spark.implicits._
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10)) // batch duration
    val lines = ssc.socketTextStream("ec2-13-233-37-66.ap-south-1.compute.amazonaws.com", 1234)// lines is d streaming
    // live data .. even based time based ..
    val res = lines.foreachRDD { x=>
    println(s"processing first line : $x")
      val data = x
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = data.map(x=>x.split(" ")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.show()

      val oUrl = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val oProp = new java.util.Properties()
      oProp.setProperty("user","ousername")
      oProp.setProperty("password","opassword")
      oProp.setProperty("driver","oracle.jdbc.OracleDriver")
      val emp = spark.read.jdbc(oUrl, "emp",oProp)
      emp.createOrReplaceTempView("emp")
      df.createOrReplaceTempView("live")
      val result = spark.sql("select l.name, l.city, e.job from emp e live l on e.name=l.name")
      df.write.mode(SaveMode.Append).jdbc(oUrl,"livedata",oProp)
    }


      //val wc = lines.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKeyAndWindow((a:Int, b:Int)=> a+b,Minutes(4),Seconds(30))

      //wc.print() // balls .. overs .. 50 overs /// batch window (10 sec) .... slide window (30 sec) ... window priod (last 4 minurwa seconds)
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}
