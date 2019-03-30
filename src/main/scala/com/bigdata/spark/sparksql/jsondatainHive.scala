/*package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import java.sql._

object jsondatainHive {
    def main(args: Array[String]) {
      //val spark = SparkSession.builder.master("local[*]").appName("jsonData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
      val spark = SparkSession.builder.master("local[*]").appName("jsonData").enableHiveSupport().getOrCreate()
      val sc = spark.sparkContext
      val conf = new SparkConf().setAppName("jsonData").setMaster("local[*]")
      //val sc = new SparkContext(conf)
      //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val sqlContext = spark.sqlContext
      import spark.implicits._
      import spark.sql

      val hivejsontab = args(0)
      val jwbData = "file:///home/hadoop/world_bank.json"
      // val jData = "C:\\work\\datasets\\jsondata\\zips.json"
      //zip dataFrame
      //val jsonDF = spark.read.format("json").option("inferSchema","true").load(jData)
      //world bank dataFrame
      val jsonwbDF = spark.read.format("json").option("inferSchema","true").load(jwbData)
      //jsonwbDF.printSchema()
      /*
      // ZIP json data
      jsonDF.createOrReplaceTempView("jsonTab")
      val res = spark.sql("select _id id,city,loc[0] langitude,loc[1] latitude,pop,state from jsonTab")
      res.createOrReplaceTempView("jsonNewData")
      val res1 = spark.sql("select state,count(*) from jsonNewData group by state")
      //jsonDF.printSchema()
      //jsonDF.show()
      //res.show()
      res1.show()
      */
      // world bank data

      jsonwbDF.createOrReplaceTempView("jsonwbTab")
      // if only array data type then you can use arr[0] aliad,arr[1] alias1....
      //if struct datatype use parent_column.child_column
      //if struct with array then use lateral view explode(column) t as tmp
      val res = spark.sql("select _id.`$oid` oid, approvalfy, board_approval_month, boardapprovaldate,borrower, closingdate," +
        "country_namecode, countrycode, countryname, countryshortname, envassesmentcategorycode, grantamt, ibrdcommamt, id, idacommamt," +
        "impagency, lendinginstr, lendinginstrtype, lendprojectcost, MJSP.Name MjspName, MJSP.Percent MjspPercent, " +
        "MJSNC.code MjsncCode, MJSNC.name MjsncName, mjtheme[0] mjtheme1, MJTNC.code MjtncCode, MJTNC.name MjtncName, mjthemecode, " +
        "prodline, prodlinetext, productlinetype ," +
        "PJDC.DocDate, PJDC.DocType, PJDC.DocTypeDesc, PJDC.DocURL, PJDC.EntityID, projectfinancialtype, projectstatusdisplay, regionname," +
        "SCTR.Name SctrName, sector1.Name Sctr1Name, sector1.Percent Sctr1Percent, sector2.Name Sctr2Name, sector2.Percent Sctr2Percent, " +
        "sector3.Name Sctr3Name, sector3.Percent Sctr3Percent, sector4.Name Sctr4Name, sector4.Percent Sctr4Percent, SCTRNC.code SctrncCode, SCTRNC.name SctrncName," +
        " source, status, supplementprojectflg, " +
        "theme1.Name themename, theme1.Percent, THNC.code Thncode, THNC.name Thnname, themecode, totalamt,totalcommamt, url " +
        "from jsonwbTab " +
        "lateral view explode(majorsector_percent) C1 as MJSP " +
        "lateral view explode(mjsector_namecode) C2 as MJSNC " +
        "lateral view explode(mjtheme_namecode) C3 as MJTNC " +
        "lateral view explode(projectdocs) C4 as PJDC " +
        "lateral view explode(sector) C5 as SCTR " +
        "lateral view explode(sector_namecode) C10 as SCTRNC " +
        "lateral view explode(theme_namecode) C11 as THNC limit 500")
      //res.show(10)
      res.printSchema()
      //now load the data in oracle
      /*
      res.createOrReplaceTempView("jsonData")
      val res1 = spark.sql("create table jsonhivetab as select * from jsonData")
      //res.show()
      res1.write.format("orc").insertInto("jsonhivetab")
      */
      res.write.format("orc").saveAsTable(hivejsontab)
      /*
      val url = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val prop = new java.util.Properties()
      prop.setProperty("user","ousername")
      prop.setProperty("password","opassword")
      prop.setProperty("driver","oracle.jdbc.OracleDriver")
      res.write.mode("append").jdbc(url,"SUNIL_JSONWBDATA",prop)
      */
      spark.stop()
    }
  }
*/