package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.hive.HiveContext

object rdbmsread {
  var sparkSession: SparkSession = null
  var conf: SparkConf = null

  def main(args: Array[String]): Unit = {
    def initializeSparkContext(): Unit = {
      val conf = new SparkConf().setAppName("rdbmsread")
      sparkSession = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
    }

    def initializeSparkLocalMode(numCores: String): Unit = {
      val conf = new SparkConf().setAppName("rdbmsread")
        .setMaster(numCores)
        .set("spark.driver.host", "localhost")
        .set("spark.ui.enabled", "false") //disable spark UI
      sparkSession = SparkSession.builder.config(conf).getOrCreate()
    }

    def initializeDataBricks(dataBricksSparkSession: SparkSession): Unit = {
      sparkSession = dataBricksSparkSession;
    }
    def stopSparkContext(): Unit = {
      sparkSession.stop()
    }

    def oracledbread(driverClassName: String, ourl: String, ouser: String, opassword: String, oquery: String,
                     odriver: String, delimiter: Option[String]): Unit = {

      val url = "ourl"
      val query = "oquery"
      val prop = new java.util.Properties()
      prop.setProperty("user", "ouser")
      prop.setProperty("password", "opassword")
      prop.setProperty("driver", "odriver")
      val df = sparkSession.read.jdbc(url, query, prop)
      df.write.format("ORC").mode("overwrite").option("path", "hdfs:///user/hive/warehouse").saveAsTable("fnboutput")
      df.show()
      sparkSession.stop()
    }

    //    def flattenDataFrame(df: DataFrame, delimiter: String): DataFrame = {
    //      //There is a bug open for this in the Spark Bug tracker
    //      val x = rdbmsread.sparkSession
    //      import x.implicits._
    //
    //      val flatLeft = df.map(row => row.mkString(delimiter)).toDF("values")
    //      return flatLeft
    //    }

  }
}





