package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object loadingdatainHive {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("loadingdatainHive").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("loadingdatainHive").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("loadingdatainHive").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val hivejointab = args(0)
    //oracle database connection
    val oraUrl = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oraProp = new java.util.Properties()
    oraProp.setProperty("user","ousername")
    oraProp.setProperty("password","opassword")
    oraProp.setProperty("driver","oracle.jdbc.OracleDriver")
    val oraDf = spark.read.jdbc(oraUrl,"EMP" ,oraProp)
    //oDf.show()
    oraDf.createOrReplaceTempView("oraEMP")
    //mysql database connection
    val myUrl = "jdbc:mysql://mysqltcsproject.ckzjezatnmoy.ap-south-1.rds.amazonaws.com:3306/MysqlDB"
    val myProp = new java.util.Properties()
    myProp.setProperty("user","musername")
    myProp.setProperty("password","mpassword")
    myProp.setProperty("driver","com.mysql.jdbc.Driver")
    val myDf = spark.read.jdbc(myUrl,"DEPT_SUNIL" ,myProp)
    //mDf.show()
    myDf.createOrReplaceTempView("myDEPT")

    val joinres = spark.sql("select E.ENAME,E.EMPNO,E.SAL,D.DEPTNO,D.LOC,D.DNAME from oraEMP E join myDEPT D on E.DEPTNO=D.DEPTNO")
    //res.show()

    //res.show()
    joinres.write.format("orc").saveAsTable(hivejointab)


    spark.stop()
  }
}
