package com.bigdata.spark.sparkcore

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.functions._
import java.io.File
import com.typesafe.config._

object DataExtraction {
  def main(args: Array[String]) {

        val start = System.currentTimeMillis()

        //<-------------------------------cassandra extract configuaration----------------------------------------------------------------->

        val conf = new SparkConf(true).setAppName("TmobilePOC").setMaster("local[*]")
        conf.set("spark.driver.allowMultipleContexts", "true")
        conf.set("spark.cassandra.connection.host", "10.135.83.44")
        conf.set("spark.cassandra.connection.port", "9042")
        conf.set("spark.cassandra.auth.username", "risp_read_only")
        conf.set("spark.cassandra.auth.password", "R!sprea30n!y")
        conf.set("spark.driver.memory", "10g")
        conf.set("spark.memory.offHeap.enabled", "true")
        conf.set("spark.memory.offHeap.size", "5g")
        conf.set("spark.executor.memory", "2g")


        val sc = new SparkContext(conf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val spark = SparkSession.builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        println("----------SOA-Cassandra extraction started------------------------------------------->")

        val soaConfig = ConfigFactory.parseFile(new File("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_scripts/soaFile.conf"))

        val soa_startTime = soaConfig.getString("startTime")

        val soa_endTime = soaConfig.getString("endTime")

        val cycle_no= soaConfig.getString("cycle_no")

        val df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "transactionnotificationlog", "keyspace" -> "risp2")).load

        df.registerTempTable("transactionnotificationlog")

        val transactionnotificationlog = sqlContext.sql(s"""select * from transactionnotificationlog WHERE createdtimestamp >= '${soa_startTime}' AND createdtimestamp <  '${soa_endTime}'""").withColumn("TRAN_STATUS", lit("TRA")).withColumn("cycle_no", lit(cycle_no)).select(col("*"), col("eventattributelist.serialNumber")).drop("eventattributelist")

        println("transactionnotificationlog Data count------------------------------------> " + transactionnotificationlog.count())

        println("transactionnotificationlog extraction successfully completed")

        val df1 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "apipayloadlog", "keyspace" -> "risp2")).load

        df1.registerTempTable("apipayloadlog")

        val apipayloadlog = sqlContext.sql(s"""select * from apipayloadlog WHERE lastupdateat >= '${soa_startTime}' AND lastupdateat <  '${soa_endTime}'""").withColumn("APIPAY_STATUS", lit("API")).withColumn("cycle_no", lit(cycle_no)).drop("payload")

        println("apipayloadlog Data count------------------------------------> " + apipayloadlog.count())

        println("apipayloadlog extraction successfully completed")

        val df2 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "returncenterreceive", "keyspace" -> "risp2")).load

        df2.registerTempTable("returncenterreceive")

        val returncenterreceive = sqlContext.sql(s"""select * from returncenterreceive WHERE createddate >= '${soa_startTime}' AND createddate <  '${soa_endTime}'""").withColumn("RETURN_STATUS", lit("RETURN")).withColumn("cycle_no", lit(cycle_no))

        println("returncenterreceive Data count------------------------------------> " + returncenterreceive.count())

        import spark.implicits._

        val returncenterreceive_explode = returncenterreceive.withColumn("skuchangelist",explode_outer($"skuchangelist")).select(col("*"), col("skuchangelist.*")).drop("skuchangelist").drop("movementtype")

        println("returncenterreceive extraction successfully completed")

        val df3 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "shipmentconfirmation", "keyspace" -> "risp2")).load

        df3.registerTempTable("shipmentconfirmation")

        val shipmentconfirmation = sqlContext.sql(s"""select * from shipmentconfirmation WHERE lastupdateddate >= '${soa_startTime}' AND lastupdateddate <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("shipmentconfirmation Data count------------------------------------> " + shipmentconfirmation.count())

        import spark.implicits._

        val shipmentconfirmation_explode = shipmentconfirmation.withColumn("shipmentitems",explode_outer($"shipmentitems")).select(col("*"), col("shipmentitems.*")).drop("shipmentitems")

        println("shipmentconfirmation extraction successfully completed")

        val df4 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "orders", "keyspace" -> "risp2")).load

        df4.registerTempTable("orders")

        val orders = sqlContext.sql(s"""select * from orders WHERE lastupdateddate >= '${soa_startTime}' AND lastupdateddate <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("orders Data count------------------------------------> " + orders.count())

        import spark.implicits._

        val orders_explode = orders.withColumn("partnerlist",explode_outer($"partnerlist")).withColumn("departmentlist",explode_outer($"departmentlist")).select(col("*"), col("partnerlist.*"),col("departmentlist.*")).drop("partnerlist").drop("departmentlist")

        println("orders extraction successfully completed")

        val df5 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "assessmentinfo", "keyspace" -> "risp2")).load

        df5.registerTempTable("assessmentinfo")

        val assessmentinfo = sqlContext.sql(s"""select * from assessmentinfo WHERE updateddate >= '${soa_startTime}' AND updateddate <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("assessmentinfo Data count------------------------------------> " + assessmentinfo.count())

        import spark.implicits._

        val assessmentinfo_explode = assessmentinfo.withColumn("assessmentresultlist",explode_outer($"assessmentresultlist")).select(col("*"), col("assessmentresultlist.*")).drop("assessmentresultlist")

        println("assessmentinfo extraction successfully completed")


        val df6 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "dockreceiptinfo", "keyspace" -> "risp2")).load

        df6.registerTempTable("dockreceiptinfo")

        val dockreceiptinfo = sqlContext.sql(s"""select * from dockreceiptinfo WHERE lastupdateddate >= '${soa_startTime}' AND lastupdateddate <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("dockreceiptinfo Data count------------------------------------> " + dockreceiptinfo.count())

        import spark.implicits._

        val dockreceiptinfo_explode = dockreceiptinfo.withColumn("trackingnumbers",explode_outer($"trackingnumbers")).withColumn("damageinformation",explode_outer($"damageinformation")).select(col("*"), col("trackingnumbers.*"),col("damageinformation.*")).drop("trackingnumbers").drop("damageinformation")

        println("dockreceiptinfo extraction successfully completed")

        val df7 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "skumaster", "keyspace" -> "risp2")).load

        df7.registerTempTable("skumaster")

        val skumaster = sqlContext.sql(s"""select * from skumaster WHERE skulastchange >= '${soa_startTime}' AND skulastchange <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("skumaster Data count------------------------------------> " + skumaster.count())

        import spark.implicits._

        val skumaster_explode = skumaster.withColumn("skucharecterlist",explode_outer($"skucharecterlist")).withColumn("skuuomlist",explode_outer($"skuuomlist")).select(col("*"), col("skucharecterlist.*"),col("skuuomlist.*")).drop("skucharecterlist").drop("skuuomlist").drop("skufunction")

        println("skumaster extraction successfully completed")

        val df8 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "inventoryadjustment", "keyspace" -> "risp2")).load

        df8.registerTempTable("inventoryadjustment")

        val inventoryadjustment = sqlContext.sql(s"""select * from inventoryadjustment WHERE createddate >= '${soa_startTime}' AND createddate <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("inventoryadjustment Data count------------------------------------> " + inventoryadjustment.count())

        val inventoryadjustment_explode = inventoryadjustment.withColumn("parameterlist",explode_outer($"parameterlist")).select(col("*"), col("parameterlist.*")).drop("parameterlist")

        println("inventoryadjustment extraction successfully completed")

        val df9 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "serializedmaterial", "keyspace" -> "risp2")).load()

        df9.registerTempTable("serializedmaterial")

        val serializedmaterial = sqlContext.sql(s"""select * from serializedmaterial WHERE lastupdatedby >= '${soa_startTime}' AND lastupdatedby <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("serializedmaterial Data count------------------------------------> " + serializedmaterial.count())

        println("serializedmaterial extraction successfully completed")

        val df10 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "serializedmateriallifecycle", "keyspace" -> "risp2")).load()

        df10.registerTempTable("serializedmateriallifecycle")

        val serializedmateriallifecycle = sqlContext.sql(s"""select * from serializedmateriallifecycle WHERE createddate >= '${soa_startTime}' AND createddate <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("serializedmateriallifecycle Data count------------------------------------> " + serializedmateriallifecycle.count())

        println("serializedmateriallifecycle extraction successfully completed")

        val df11 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "serializedmateriallifecyclereport", "keyspace" -> "risp2")).load()

        df11.registerTempTable("serializedmateriallifecyclereport")

        val serializedmateriallifecyclereport = sqlContext.sql(s"""select * from serializedmateriallifecyclereport WHERE createddate >= '${soa_startTime}' AND createddate <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("serializedmateriallifecyclereport Data count------------------------------------> " + serializedmateriallifecyclereport.count())

        println("serializedmateriallifecyclereport extraction successfully completed")

        val df12 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "stockonhand", "keyspace" -> "risp2")).load()


        df12.registerTempTable("stockonhand")

        val stockonhand = sqlContext.sql(s"""select * from stockonhand WHERE lastupdateddate >= '${soa_startTime}' AND lastupdateddate <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        val stockonhanddf_explode = stockonhand.withColumn("materialset", concat_ws(",", $"materialset"))

        println("stockonhand Data count------------------------------------> " + stockonhand.count())

        println("stockonhand extraction successfully completed")

        val stockonhanddf_final = stockonhanddf_explode.withColumn("materialset", explode_outer(split($"materialset","[,]")))

        val expectedserializedmaterial = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "expectedserializedmaterial", "keyspace" -> "risp2")).load()

        expectedserializedmaterial.registerTempTable("expectedserializedmaterial")

        println("expectedserializedmaterial Data count------------------------------------> " + expectedserializedmaterial.count())

        println("expectedserializedmaterial extraction successfully completed")

        val df14 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "nonserializedmaterial", "keyspace" -> "risp2")).load()

        df14.registerTempTable("nonserializedmaterial")

        val nonserializedmaterial = sqlContext.sql(s"""select * from nonserializedmaterial WHERE lastupdateddate >= '${soa_startTime}' AND lastupdateddate <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("nonserializedmaterial Data count------------------------------------> " + nonserializedmaterial.count())

        println("nonserializedmaterial extraction successfully completed")

        val df15 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "pistoreserializedmaterial", "keyspace" -> "risp2")).load()

        df15.registerTempTable("pistoreserializedmaterial")

        val pistoreserializedmaterial = sqlContext.sql(s"""select * from pistoreserializedmaterial WHERE lastupdateddate >= '${soa_startTime}' AND lastupdateddate<  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))

        println("pistoreserializedmaterial Data count------------------------------------> " + pistoreserializedmaterial.count())

        println("pistoreserializedmaterial extraction successfully completed")

        val df16 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "pistorenonserializedmaterial", "keyspace" -> "risp2")).load()

        df16.registerTempTable("pistorenonserializedmaterial")

        val pistorenonserializedmaterial = sqlContext.sql(s"""select * from pistorenonserializedmaterial WHERE lastupdateddate >= '${soa_startTime}' AND lastupdateddate <  '${soa_endTime}'""").withColumn("cycle_no", lit(cycle_no))
        println("pistorenonserializedmaterial Data count------------------------------------> " + pistorenonserializedmaterial.count())

        println("pistorenonserializedmaterial extraction successfully completed")

    transactionnotificationlog.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/transactionnotificationlog/")

    apipayloadlog.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/apipayloadlog/")

    returncenterreceive_explode.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/returncenterreceive/")

    shipmentconfirmation_explode.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/shipmentconfirmation/")

    orders_explode.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/orders/")

    assessmentinfo_explode.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/assessmentinfo/")

    dockreceiptinfo_explode.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/dockreceiptinfo/")

    skumaster_explode.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/skumaster/")

    inventoryadjustment_explode.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/inventoryadjustment/")

    serializedmaterial.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/serializedmaterial/")

    serializedmateriallifecycle.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/serializedmateriallifecycle/")

    serializedmateriallifecyclereport.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/serializedmateriallifecyclereport/")

    stockonhanddf_final.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/stockonhanddf/")

    expectedserializedmaterial.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/expectedserializedmaterial/")

    nonserializedmaterial.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/nonserializedmaterial/")

    pistoreserializedmaterial.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/pistoreserializedmaterial/")

    pistorenonserializedmaterial.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","").option("delimiter","^").save("/app/UDMF/REVERSE_LOGISTICS/RL_RECON/rl_recon_inbound/pistorenonserializedmaterial/")


      }
    }
