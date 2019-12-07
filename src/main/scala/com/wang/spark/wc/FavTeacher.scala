package com.wang.spark.wc

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 做出最受欢迎的老师是谁？
 */
object FavTeacher {
  def main(args: Array[String]): Unit = {
    //本地运行模式，也可以读取HDFS伤的数据
    //F:\input
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //指定以后从里读取数据
    val lines: RDD[String] = sc.textFile(args(0))
    //整理数据
    val teacherAndOne = lines.map(line =>{
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index+1)
      //val httpHost = line.substring(0,index)
      //val subject = new URL(httpHost).getHost.split("[.]")(0)
      (teacher,1)
    })
    //集合
    val reduced: RDD[(String,Int)] = teacherAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String,Int)] = reduced.sortBy(_._2,false)
    //执行计算
    val results: Array[(String,Int)] = sorted.collect()

    //打印
    println(results.toBuffer)

    //关闭
    sc.stop()
  }
}
