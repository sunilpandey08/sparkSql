package com.bigdata.spark.sparkcore

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import scala.util.Try

object filesrename {
  def main(args: Array[String]) {
    import java.io.File
    def getListOfFiles(dir: String): List[String] = {
      val file = new File(dir)
      file.listFiles.filter(_.isFile).filter(_.getName.startsWith("my")).map(_.getPath).toList
      
      //file.foreach(println)
    }
    val ioDirectory = "C:\\Users\\pande\\Desktop\\test\\"
    val r = scala.util.Random
    val filename = getListOfFiles(ioDirectory).toList
    implicit def file(s: String) = new File(s)
    for (i <- filename) {
      //Here you can give time stamp also in place of random number
      new File(i).renameTo(new File(ioDirectory+ r.nextInt +"_sunil.txt"))
      println(i)
    }
  }
}
