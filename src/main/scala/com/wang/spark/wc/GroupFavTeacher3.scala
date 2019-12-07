package com.wang.spark.wc

import java.net.URL
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import scala.collection.mutable

/**
 * 做出最受欢迎的老师是谁？
 */
object GroupFavTeacher3 {
  def main(args: Array[String]): Unit = {
    //本地运行模式，也可以读取HDFS伤的数据
    //F:\input
    val conf = new SparkConf().setAppName("FavTeacher3").setMaster("local[4]")
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

    //计算有多少学科？？？
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
    /**
     * 自定义一个分区器：按照指定的分区进行分区
     */
    val sbPartitioner =  new SubjectPartitioner(subjects)
    //按照指定的分区进行分区
    val partitioned: RDD[((String,String),Int)] = reduced.partitionBy(sbPartitioner)

    //如果一次操作一个分区中的数据
    val sorted: RDD[((String,String),Int)] = partitioned.mapPartitions(it =>{
      it.toList.sortBy(_._2).reverse.take(3).iterator
    })

    val r: Array[((String,String),Int)] = sorted.collect()
    //释放资源
    sc.stop()
  }
}



class SubjectPartitioner(sbs: Array[String]) extends Partitioner{
  //存放规则的map
  val rules = new mutable.HashMap[String,Int]()
  var i = 0
  for(sb <- sbs){
    rules(sb) = i
  }
  //返回分区的数量 下一个RDD有多少分区
  override def numPartitions: Int = sbs.length
  //根据传入的key就计算分区编号
  override def getPartition(key: Any): Int = {
    //拿到的学科名称
    val subject = key.asInstanceOf[(String,String)]._1
    //根据规则计算分区编号
    rules(subject)
  }
}
