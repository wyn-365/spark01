package com.wang.spark.wc

object TestSplit {
  def main(args: Array[String]): Unit = {
    val line = "http://bigdata.edu360.cn/laozhang"
    //学科  老师
    val splits: Array[String] = line.split("/")

    val subject = splits(2).split("[.]")(0)
    val teacher = splits(3)

    println(subject+" "+teacher)
  }
}
