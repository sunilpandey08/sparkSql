package com.bigdata.spark.sparkcore

object wordcount {
  def main(args: Array[String]) {
    val v1 = List("you are my hero and icon")
    var v2 = 0
    for (i <- v1.flatMap(x=>x.split(" "))) {
      v2=v2+1
    }
println(v2)

  }
}
