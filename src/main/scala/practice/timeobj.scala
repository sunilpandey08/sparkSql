package practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object timeobj {
  def main(args: Array[String]) {
    val cycle = new getcycleno
    println(cycle.cycle_no)
    println(cycle.startDate)
    println(cycle.endDate)
  }
}
