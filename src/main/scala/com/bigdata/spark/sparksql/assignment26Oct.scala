package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel._

object assignment26Oct {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("assignment26Oct").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.appName("assignment26Oct").getOrCreate()

    //val conf = new SparkConf().setAppName("assignment26Oct").setMaster("local[*]")
    /*
    val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
    */
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //oracle database connection
      //val s3data = args(0)
      val mysqltab = args(0)
    val orUrl = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val orProp = new java.util.Properties()
    orProp.setProperty("user","ousername")
    orProp.setProperty("password","opassword")
    orProp.setProperty("lowerBound","1")
    orProp.setProperty("upperBound","10000")
    orProp.setProperty("numPartitions","7000")
    orProp.setProperty("partitionColumn", "FLIGHTNUM")
    orProp.setProperty("driver","oracle.jdbc.OracleDriver")

    //mysql database connection
    val mUrl = "jdbc:mysql://mysqltcsproject.ckzjezatnmoy.ap-south-1.rds.amazonaws.com:3306/MysqlDB"
    val mProp = new java.util.Properties()
    mProp.setProperty("user","musername")
    mProp.setProperty("password","mpassword")
    mProp.setProperty("lowerBound","1")
    mProp.setProperty("upperBound","10000")
    mProp.setProperty("numPartitions","7000")
    mProp.setProperty("partitionColumn", "FLIGHTNUM")
    mProp.setProperty("driver","com.mysql.jdbc.Driver")

    //reading data from oracle
    val query = """(select * from AIRLINEDATA where FLIGHTNUM=335) AIRLINEDATA """

    val oorDf = spark.read.jdbc(orUrl,query,orProp)


    oorDf.createOrReplaceTempView("airlineoracle")
    oorDf.persist(MEMORY_AND_DISK_SER)
    //val oorDf = spark.read.jdbc(orUrl,"EMP" ,orProp)
    //oDf.take(10)
    oorDf.printSchema()

    //val res1 = spark.sql("select * from airlineoracle ")
   // res1.show()
    //val data ="s3n://sunil008/input/2008.csv"
    //val data = "C:\\work\\datasets\\2008.csv"
    val data ="file:///home/wce/clsadmin/data/*.*"
    val df = spark.read.format("csv").option("delimiter",",").option("header","true").option("inferSchema","true").load(data)

    df.createOrReplaceTempView("csvtab")
    df.persist(MEMORY_AND_DISK_SER)
    val res = spark.sql("select ort.Year,ort.Month,ort.DayofMonth,ort.DayOfWeek,ort.DepTime,ort.CRSDepTime,ort.ArrTime,ort.CRSArrTime," +
      "ort.UniqueCarrier,ort.FlightNum,ort.TailNum,ort.ActualElapsedTime,ort.CRSElapsedTime,s3f.AirTime,s3f.ArrDelay,s3f.DepDelay,s3f.Origin,s3f.Dest," +
      "s3f.Distance,s3f.TaxiIn,s3f.TaxiOut," +
      "s3f.Cancelled,s3f.CancellationCode,s3f.Diverted,s3f.CarrierDelay,s3f.WeatherDelay,s3f.NASDelay,s3f.SecurityDelay,s3f.LateAircraftDelay" +
      " from airlineoracle ort join csvtab s3f on ort.YEAR = s3f.YEAR")
    //res.show()
    res.write.mode("overwrite").jdbc(mUrl,mysqltab,mProp)

    spark.stop()
  }
}
