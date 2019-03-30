package com.bigdata.spark.practiceScala

//class student {
class student(ID:Int=0,Name:String=null) {
def show(): Unit = {
  println(ID + " " + Name)
}
}
object mainObject {
  def main(args:Array[String]): Unit = {
    //var s = new student()
    var s = new student(101,"sunil")
    s.show()
  }
}


object findlargestnbr {
  def main(args:Array[String]) {
    var num1 = 20
    var num2 = 30
    var x = 10
    if (num1>num2) {
      println("Largest number is : " + num1)
    }
    else {
      println("Largest number is : " + num2)
    }
  }
}


