/*
package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector


object oct23task1 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("oct23task1").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("oct23task1").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("oct23task1").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val mUrl = "jdbc:mysql://mysqltcsproject.ckzjezatnmoy.ap-south-1.rds.amazonaws.com:3306/MysqlDB"
    val mProp = new java.util.Properties()
    mProp.setProperty("user","musername")
    mProp.setProperty("password","mpassword")
    mProp.setProperty("driver","com.mysql.jdbc.Driver")

    val query = """(SELECT table_name FROM information_schema.tables where table_schema='MysqlDB') tablename """
    val mDf = spark.read.jdbc(mUrl,query,mProp)
    val alltab  = mDf.select("TABLE_NAME").rdd.map(r => r(0)).collect.toArray

    /*
    mDf.createOrReplaceTempView("tab")
    val res = spark.sql("select table_name from tab")
    //val lst= mDf.collect().foreach(println)
    val tab1 = res.collect.toArray
    */
    for (tname <- alltab) {
      println(tname)
      val query1 = s"(SELECT * FROM $tname) tname"
      println(query1)
      val m1Df = spark.read.jdbc(mUrl,query1,mProp)
      m1Df.show()
      .saveAsCassandraTable
      val df = spark.read.format("org.apache.spark.sql.cassandra").
      df.show()
      println("testing")
      createCassandraTable("cassdb1",tname.toString())
      println("testing1")
      df.write.mode("overwrite").format("org.apache.spark.sql.cassandra").option("keyspace","cassdb1").option("table",tname.toString()).insertInto(tname.toString())
    }

    logitems.foreachRDD(items => {
      if (items.count() == 0)
        println("No log item received")
      else{
        val first = items.first()
        println(first.timestamp)  // WORKS: Shows the timestamp in the first rdd element

        items.saveAsCassandraTable("analytics", "test_logs", SomeColumns("timestamp", "c_ip", "c_referrer", "c_user_agent"))
        //table schema is created but the rdd items are not written
      }
    })

    spark.stop()
  }
}
*/
