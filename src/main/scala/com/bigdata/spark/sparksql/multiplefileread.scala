package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object multiplefileread {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("oct23task2").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("multiplefileread").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("multiplefileread").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val data = "C:\\Users\\pande\\Desktop\\test\\"

      val filename = sc.wholeTextFiles(data)
    filename.coalesce(1).saveAsTextFile("C:\\Users\\pande\\Desktop\\newfile\\")



    /*
    val colms = df.columns.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+", ""))
    val colDF = df.toDF(colms:_*)
    colDF.printSchema()
    //colDF.show()
    val thirty = List("NY","OK","TX","CA","AK","WV")
    val twenty = List("OK","DC","FL","IA")
    val res = colDF.select(concat_ws(" ",$"FirstName",$"LastName").alias("FullName"),$"State",$"Salary",$"EMail").withColumn("extraBonus",when($"EMail".contains("ibm"),when($"State".isin(thirty: _*), bround(($"Salary"*1.3),2)).when($"State".isin(twenty: _*), bround(($"Salary"*1.2),2)).otherwise(bround(($"Salary"*1.1),2)))).withColumn("extraHikePercent",when($"EMail".contains("ibm"),when($"State".isin(thirty: _*), "30% hike").when($"State".isin(twenty: _*), "20% hike").otherwise("10% hike"))).filter($"Email".contains("ibm"))

    val res2 = colDF.select($"EmpID",$"FirstName",$"LastName",$"DateofBirth",date_format(to_date(col("DateofBirth"), "MM/dd/yy"), "yyyy-MM-dd").alias("DOB")).orderBy(datediff(current_date(),$"dob") desc).limit(10)
    res2.show()

    val res3 = colDF.select($"EmpID",$"FirstName",$"LastName",$"DateofJoining",date_format(to_date(col("DateofJoining"), "MM/dd/yy"), "yyyy-MM-dd").alias("DOJ")).orderBy(datediff(current_date(),$"DOJ") desc).limit(10)
    res3.show(50)
*/
    spark.stop()
  }
}
