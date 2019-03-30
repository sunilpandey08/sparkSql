package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object cassandraTableCreate {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("cassandraTableCreate").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("cassandraTableCreate").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("cassandraTableCreate").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    import com.datastax.spark.connector.cql.CassandraConnector
    /*
    val connector = CassandraConnector(csc.conf)
    val session = connector.openSession()
    session.execute(s"DROP TABLE IF EXISTS spark_ex2.sftmax")
    session.execute(s"CREATE TABLE IF NOT EXISTS spark_ex2.sftmax(location TEXT, year INT, month INT, day INT, tmax DOUBLE, datestring TEXT, PRIMARY KEY ((location), year, month, day)) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)")
    case class Tmax(location: String, year: Int, month: Int, day: Int, tmax: Double, datestring: String)
    val tmax_raw = sc.textFile("webhdfs://sandbox.hortonworks.com:50070/user/guest/data/sftmax.csv")
    val tmax_c10 = tmax_raw.map(x=>x.split(",")).map(x => Tmax(x(0), x(1).toInt, x(2).toInt, x(3).toInt, x(4).toDouble, x(5)))
    tmax_c10.count
    tmax_c10.saveToCassandra("spark_ex2", "sftmax")
    session.execute("SELECT COUNT(*) FROM spark_ex2.sftmax").all.get(0).getLong(0)
    */
    spark.stop()
  }
}
