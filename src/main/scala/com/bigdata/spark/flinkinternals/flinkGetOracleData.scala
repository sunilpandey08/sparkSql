package com.bigdata.spark.flinkinternals

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.CsvTableSource
import java.math._
//Caused by: java.lang.ClassCastException: java.math.BigDecimal cannot be cast to scala.math.BigDecimal
object flinkGetOracleData {
  case class deptcc(name:String, age:BigDecimal, city:String)

  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment

    val tEnv = TableEnvironment.getTableEnvironment(env)
    System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true")

    var fieldTypes: Array[TypeInformation[_]] = Array(createTypeInformation[String],createTypeInformation[BigDecimal],  createTypeInformation[String])
    var fieldNames: Array[String] = Array("name", "age","city")

    val rowTypeInfo = new RowTypeInfo(
      fieldTypes,
      fieldNames
    )
    import org.apache.flink.table.api.{TableEnvironment, Types}
    val inputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("oracle.jdbc.OracleDriver")
      .setDBUrl("jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL")
      .setUsername("ousername")
      .setPassword("opassword")
      .setQuery("SELECT * FROM asl")
      .setRowTypeInfo(rowTypeInfo)
      .finish()

    val ds = env.createInput(inputFormat)
    tEnv.registerDataSet("tab", ds)
    val res = tEnv.sqlQuery("select * from tab").toDataSet[deptcc]
    res.print()
    // export data



  }
}
