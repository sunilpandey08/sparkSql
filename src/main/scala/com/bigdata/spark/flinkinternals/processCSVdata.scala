package com.bigdata.spark.flinkinternals

import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.api.scala._

object processCSVdata {
  case class aslcc(name:String, age:Int, city:String)
  def main(args: Array[String]) {

    // environment configuration
   val env=ExecutionEnvironment.getExecutionEnvironment
  //  val env = StreamExecutionEnvironment.getExecutionEnvironment


    val tEnv = TableEnvironment.getTableEnvironment(env)
    val data = "C:\\work\\datasets\\asl.csv"
    val ds :DataSet[aslcc] = env.readCsvFile(data,"\n",",",ignoreFirstLine=true)
    tEnv.registerDataSet("tab",ds)
    val res = tEnv.sqlQuery("select * from tab ")

//res.writeAsCsv("C:\\work\\datasets\\output\\result.csv",fieldDelimiter = "|").setParallelism(1)
val jdbcSink=JDBCAppendTableSink.builder()
  .setDrivername("oracle.jdbc.OracleDriver")
  .setDBUrl("jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL")
  .setUsername("ousername")
  .setPassword("opassword")
  .setQuery("insert into asl (name,age,city) values(?,?,?)")
  .setParameterTypes( Types.STRING,Types.INT, Types.STRING)
  .build()

    res.writeToSink(jdbcSink)
    env.execute()
    //res.print()
  }
}
