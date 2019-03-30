package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object singleton {
  def main(args: Array[String]) {
    singletonObject.method1()
  }
}

object singletonObject {
  def method1(){
    println("Hello this is singleton object")

  }
}
