//package com.bigdata.spark.sparksql
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import com.bigdata.spark.sparkcore.Initialise
//
//class dbJoinmysqloracle {
//  def test(t2:String):Initialise;
//  val t1 = new Initialise
//
//  def main(args: Array[String]) {
//    //val spark = SparkSession.builder.master("local[*]").appName("dbJoinMySqlORacle").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
//    val spark = SparkSession.builder.master("local[*]").appName("dbJoinmysqloracle").getOrCreate()
//    val sc = spark.sparkContext
//    val conf = new SparkConf().setAppName("dbJoinmysqloracle").setMaster("local[*]")
//    //    val sc = new SparkContext(conf)
//    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    val sqlContext = spark.sqlContext
//    //val hivetab = args(0)
//
//    //oracle database connection
//    val oUrl = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"
//    val oProp = new java.util.Properties()
//    oProp.setProperty("user","ousername")
//    oProp.setProperty("password","opassword")
//    oProp.setProperty("driver","oracle.jdbc.OracleDriver")
//    //oProp.setProperty("sessionInitStatement","ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MM-YYYY'")
//    val oDf = spark.read.jdbc(oUrl,"EMP" ,oProp)
//    //oDf.show()
//    oDf.createOrReplaceTempView("oEMP")
//
//    //mysql database connection
//    val mUrl = "jdbc:mysql://mysqltcsproject.ckzjezatnmoy.ap-south-1.rds.amazonaws.com:3306/MysqlDB"
//    val mProp = new java.util.Properties()
//    mProp.setProperty("user","musername")
//    mProp.setProperty("password","mpassword")
//    mProp.setProperty("driver","com.mysql.jdbc.Driver")
//78
//    val mDf = spark.read.jdbc(mUrl,"DEPT_SUNIL" ,mProp)
//    //mDf.show()
//    mDf.createOrReplaceTempView("mDEPT")
//
//
//    val res = spark.sql("select E.ENAME,E.EMPNO,E.SAL,D.DEPTNO,D.LOC,D.DNAME from oEMP E join mDEPT D on E.DEPTNO=D.DEPTNO")
//    res.show()
//    /*
//    res.createOrReplaceTempView("empdept")
//    val res1 = spark.sql("create table empjoindept as select * from empdept")
//
//    res1.write.format("orc").insertInto("empjoindept")
//    */
//    spark.stop()
//  }
//}
