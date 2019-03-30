package com.bigdata.spark.sparkcore

  import org.apache.spark.sql.SparkSession
  import java.io.File
  import com.typesafe.config._
  import java.text.SimpleDateFormat
  import java.util.Calendar
  import java.util.Properties
  import scala.collection.mutable.Map
  import scala.collection.mutable.ListBuffer
  import java.io.FileWriter
  import java.io.BufferedWriter
  import java.io.PrintWriter
  import java.io.FileReader
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.SaveMode
  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.types.StructField
  import org.apache.spark.sql.types.StringType
  import org.apache.spark.sql.types.DateType
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf

  object TransID {

    def main(args: Array[String]): Unit = {

      val start = System.currentTimeMillis()
      val conf = new SparkConf(true).setAppName("TmobilePOC").setMaster("local[*]")


      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val spark = SparkSession.builder.getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      import org.apache.spark.sql.types._

      val Schemaname = StructType(Array(StructField("no", StringType, true), StructField("f1", StringType, true),StructField("f2", StringType, true),StructField("f3", StringType, true)))
      val df1 = spark.read.format("csv").option("delimiter",",").option("header","true").schema(Schemaname).load("C:\\work\\datasets\\output\\asl.csv")
      val df2=df1.withColumn("finalflag", when((col("f1")==="Y") && (col("f2")==="Y") && (col("f3")==="Y"),"T1")
       .when((col("f1")==="Y") && (col("f2")==="Y") && (col("f3")==="N"),"T2")
       .when((col("f1")==="Y") && (col("f2")==="N") && (col("f3")==="N"),"T3")
       .when((col("f1")==="N") && (col("f2")==="N") && (col("f3")==="N"),"T4")
       .when((col("f1")==="N") && (col("f2")==="N") && (col("f3")==="Y"),"T5")
       .when((col("f1")==="N") && (col("f2")==="Y") && (col("f3")==="Y"),"T6")
       .when((col("f1")==="N") && (col("f2")==="Y") && (col("f3")==="N"),"T7")
       .when((col("f1")==="Y") && (col("f2")==="N") && (col("f3")==="Y"),"T8")).select("*")

      df2.show()


    }
  }
