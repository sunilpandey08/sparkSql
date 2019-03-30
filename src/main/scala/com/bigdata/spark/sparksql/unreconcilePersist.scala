package com.bigdata.spark.sparksql

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

object unreconcilePersist {

  def main(args: Array[String]): Unit = {

    val start = System.currentTimeMillis()
    val conf = new SparkConf(true).setAppName("unreconcilePersist").setMaster("local[*]")
    /*  conf.set("spark.driver.memory", "10g")
      conf.set("spark.memory.offHeap.enabled", "true")
      conf.set("spark.memory.offHeap.size", "5g")
      conf.set("spark.executor.memory", "2g")*/

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import org.apache.spark.sql.types._

    val tranSchema = StructType(Array(StructField("aadharno", StringType, true), StructField("name", StringType, true), StructField("mobile", StringType, true), StructField("email", StringType, true)))

    val apiSchema = StructType(Array(StructField("personalID", StringType, true), StructField("country", StringType, true), StructField("city", StringType, true), StructField("state", StringType, true), StructField("zipcode", StringType, true)))

    val returnSchema = StructType(Array(StructField("socialID", StringType, true), StructField("personalemail", StringType, true), StructField("offemail", StringType, true), StructField("mobileno", StringType, true), StructField("landline", StringType, true)))


    val transnotif_df = spark.read.format("csv").option("delimiter", ",").option("header", "true").schema(tranSchema).load("C:\\work\\datasets\\transactionnotificationlog\\")

    val apipay_df = spark.read.format("csv").option("delimiter", ",").option("quote", "").schema(apiSchema).load("C:\\work\\datasets\\apipayloadlog\\")

    val return_df = spark.read.format("csv").option("delimiter", ",").option("quote", "").schema(returnSchema).load("C:\\work\\datasets\\returncenterreceive\\")


    val f_trannotif_df = transnotif_df.filter(col("aadharno").contains("368410030177")).withColumnRenamed("aadharno", "joiningkey")

    val f_apipay_df = apipay_df.filter(col("personalID").contains("368410030177")).withColumnRenamed("personalID", "joiningkey")

    import org.apache.spark.sql.functions.{concat, lit}
    import spark.implicits._

    val joined_df = f_trannotif_df.join(f_apipay_df, Seq("joiningkey"), "full_outer").select(col("*"), concat($"joiningkey", lit("_"), $"country").alias("ReCon_FLAG"))

    val returnJoin_df = joined_df.as("d1").join(return_df.as("d2"), col("d1.joiningkey") === col("d2.socialID"), "full_outer").select(col("*"), concat($"ReCon_FLAG", lit("_"), $"mobileno").alias("Final_ReCon_FLAG"))

    val newNames = Seq("joiningkey", "name", "mobile", "email", "state", "country", "city", "state", "zipcode", "personalemail", "offemail", "mobileno", "landline", "ReCon_FLAG", "Final_ReCon_FLAG")

    val dfRenamedDf = returnJoin_df.toDF(newNames: _*)

    //val finalDf = dfRenamedDf.filter(dfRenamedDf("ondemandretryflag") ==="false").drop("responsebody")


    val ReconciledDF = dfRenamedDf.filter(dfRenamedDf("Final_ReCon_FLAG") === "368410030177_india_9591548973")
    val oldUnReconciledDF = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("C:\\work\\datasets\\UnReconciled\\")
    val joinReconsile_unconsile = ReconciledDF.as("r1").join(oldUnReconciledDF.as("u1"), col("r1.joiningkey") === col("u1.joiningkey"), "left_outer").select(col("*"))
    joinReconsile_unconsile.show()

    /*val UnReconciledDF= ReconciledDF.withColumn("RECON_STATUS", when(col("Final_ReCon_FLAG").isNull && (col("TRAN_STATUS") === "TRA" && col("APIPAY_STATUS").isNull && col("RETURN_STATUS").isNull ), "MISSING IN APIPAYLOADLOG and RETURNS"))

         val UnReconciledDF1 = UnReconciledDF.withColumn("RECON_STATUS", when(col("Final_ReCon_FLAG").isNull && (col("TRAN_STATUS").isNull && col("APIPAY_STATUS") === "API" &&  col("RETURN_STATUS").isNull), "MISSING IN TRANSACTION_LOG and RETURNS"))

         val UnReconciledDF2 = UnReconciledDF1.withColumn("RECON_STATUS", when(col("Final_ReCon_FLAG").isNull && (col("TRAN_STATUS").isNull  && col("APIPAY_STATUS").isNull && col("RETURN_STATUS") === "RETURN" ), "MISSING IN APIPAYLOADLOG and TRANSACTIONLOG"))*/

    //UnReconciledDF2.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_report/UnReconciled/")

    val UnReconciledDF = joinReconsile_unconsile.withColumn("RECON_STATUS", when(col("Final_ReCon_FLAG").isNull && (col("city").isNotNull && col("mobileno").isNull && col("zipcode").isNull), "MISSING IN APIPAYLOADLOG and RETURNS"))
      .withColumn("RECON_STATUS", when(col("Final_ReCon_FLAG").isNull && (col("city").isNull && col("mobileno").isNotNull && col("zipcode").isNull), "MISSING IN TRANSACTION_LOG and RETURNS"))
      .withColumn("RECON_STATUS", when(col("Final_ReCon_FLAG").isNull && (col("city").isNull && col("mobileno").isNull && col("zipcode").isNotNull), "MISSING IN APIPAYLOADLOG and TRANSACTIONLOG"))
      .withColumn("RECON_STATUS", when(col("Final_ReCon_FLAG").isNull && (col("city").isNotNull && col("mobileno").isNotNull && col("zipcode").isNull), "MISSING IN RETURNS"))
      .withColumn("RECON_STATUS", when(col("Final_ReCon_FLAG").isNull && (col("city").isNotNull && col("mobileno").isNull && col("zipcode").isNotNull), "MISSING IN APIPAYLOAD_LOG"))


    joinReconsile_unconsile.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("quote", "").option("delimiter", "^").save("C:\\work\\datasets\\rl_recon_report_reconciled\\")
    UnReconciledDF.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("quote", "").option("delimiter", "^").save("C:\\work\\datasets\\UnReconciled\\")


  }
}
