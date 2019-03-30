package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparkFunction {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkFunction").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkFunction").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkFunction").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    val data = "C:\\work\\datasets\\mysqldata.csv"
    val csvDf = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",",").load(data)
    //csvDf.show()
    //"RANK" -- DSL command
    val deptPartWindow = Window.partitionBy($"deptno")//.orderBy($"sal".desc)
    csvDf.createOrReplaceTempView("emp")
    /*
    val rankSal = rank().over(deptPartWindow)
    csvDf.select($"*", rankSal as "SalRank").show()
    //"RANK" --  SQL query

    val sDF = spark.sql("select empno,deptno,sal,RANK() over (partition by deptno order by sal desc) rank from emp")
    sDF.show()
    //"DESNSE_RANK() -- using DSL command"
    val denseRnkSal = dense_rank().over(deptPartWindow)
    csvDf.select($"*",denseRnkSal as "denseRankSal" ).show()
    //"DESNSE_RANK() -- using SQL"
    val denseDF = spark.sql("select empno,ename,deptno,sal,DENSE_RANK() over (partition by deptno order by sal desc) dnsRnk from emp")
    denseDF.show()
    //row_number() -- using DSL commands withColumn
    csvDf.withColumn("rwnum",row_number().over(deptPartWindow)).show()
    //row_number() -- using SQL query
    val rwnmDF = spark.sql("select empno,ename,deptno,sal,row_number() over (partition by deptno order by sal desc) rwnm from emp")
    rwnmDF.show()

    //percent_rank  -- using DSL commands withColumn
    csvDf.withColumn("perRnk",percent_rank().over(deptPartWindow)).show()
    //percent_rank  -- using SQL query
    val pernkDF = spark.sql("select empno,ename,deptno,sal,percent_rank() over(partition by deptno order by sal desc) perrank from emp")
    pernkDF.show()

    //ntile -- using DSL commands withcolumn
    csvDf.withColumn("ntilecol",ntile(2).over(deptPartWindow)).show()
    //ntile -- using SQL query
    val ntilDF = spark.sql("select empno,ename,deptno,sal,ntile(2) over(partition by deptno order by sal desc) ntileval from emp")
    ntilDF.show()

    //aggregate function | sum | min | max | avg |  using DSL commands
    csvDf.withColumn("totalsal",sum($"sal").over(deptPartWindow)).show()
    csvDf.withColumn("avgsal",avg($"sal").over(deptPartWindow)).show()
    csvDf.withColumn("minsal",min($"sal").over(deptPartWindow)).show()
    csvDf.withColumn("maxsal",max($"sal").over(deptPartWindow)).show()
    //aggregate function | sum | min | max | avg | using sql query
    val sumDF = spark.sql("select empno,ename,deptno,sal,sum(sal) over (partition by deptno) totalsal from emp")
    sumDF.show()
    val avgDF = spark.sql("select empno,ename,deptno,sal,avg(sal) over (partition by deptno ) avgsal from emp")
    avgDF.show()
    val minDF = spark.sql("select empno,ename,deptno,sal,min(sal) over (partition by deptno) minsal from emp")
    minDF.show()
    val maxDF = spark.sql("select empno,ename,deptno,sal,max(sal) over (partition by deptno ) maxsal from emp")
    maxDF.show()
*/
    //analytical function | LEAD | LAG | FIRST_VALUE | LAST_VALUE |
    csvDf.withColumn("leadcol",lead($"sal",1).over(deptPartWindow)).show()
    csvDf.withColumn("lagcol",lag($"sal",1).over(deptPartWindow)).show()
    csvDf.withColumn("firstvalcol",first($"sal").over(deptPartWindow)).show()
    csvDf.withColumn("lagcol",lag($"sal",1).over(deptPartWindow)).show()

    spark.stop()
  }
}
