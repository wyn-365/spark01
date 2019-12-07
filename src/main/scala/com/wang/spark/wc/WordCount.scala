package com.wang.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object WordCount {

  def main(args: Array[String]): Unit = {

    //创建spark执行的入口
    val conf = new SparkConf().setAppName("ScalaWordCount")
    val sc = new SparkContext(conf)
    //创建RDD
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))  }

    val lines: RDD[String] = sc.textFile(args(0))
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //单词和一
    val wordAndOne: RDD[(String,Int)] = words.map((_,1))
    //按照key聚合
    val reduced: RDD[(String,Int)] = wordAndOne.reduceByKey(_+_)
    //排序
    val sorted:RDD[(String,Int)] = reduced.sortBy(_._2,false)
    //把结果保存到HDFS
    sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()

  }



}
