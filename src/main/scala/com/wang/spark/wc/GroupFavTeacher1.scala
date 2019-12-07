package com.wang.spark.wc

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 做出最受欢迎的老师是谁？
 */
object GroupFavTeacher1 {
  def main(args: Array[String]): Unit = {
    //本地运行模式，也可以读取HDFS伤的数据
    //F:\input
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //指定以后从里读取数据
    val lines: RDD[String] = sc.textFile(args(0))
    //整理数据
    val subjectTeacherAndOne: RDD[((String,String),Int)] = lines.map(line =>{
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index+1)
      val httpHost = line.substring(0,index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject,teacher),1)
    })

    //聚合，把学科和老师联合起来当做key
    val reduced:RDD[((String,String),Int)] = subjectTeacherAndOne.reduceByKey(_+_)
    //局部排序 分组排序  按照学科进行分组
    val grouped: RDD[(String,Iterable[((String,String),Int)])] = reduced.groupBy(_._1._1)
    //一个学科就是一个迭代器  把每一个组拿出来 前三个
    val sorted = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))
    //收集结果
    var r: Array[(String,List[((String,String),Int)])] = sorted.collect()

    //打印
    println(r.toBuffer)
    //释放资源
    sc.stop()
  }
}
