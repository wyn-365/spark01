package com.wang.spark.sparkstring

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 实时计算的案例
 * 计算服务器当前批次的单词个数
 * 会一直运行，除非手动截止
  * Created by zx on 2017/10/17.
  */
object SteamingWordCount {

  def main(args: Array[String]): Unit = {

    //离线任务是创建SparkContext
    //现在要实现实时计算，用StreamingContext

    val conf = new SparkConf().setAppName("SteamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //StreamingContext是对SparkContext的包装，包了一层就增加了实时的功能
    //第二个参数是小批次产生的时间间隔  实验5s
    val ssc = new StreamingContext(sc, Milliseconds(5000))

    //有了StreamingContext，就可以创建SparkStreaming的抽象了DSteam
    //例如：从一个socket端口中读取数据
    //在Linux上用yum安装nc
    //yum install -y nc
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.52.200", 8888)
    //对DSteam进行操作，你操作这个抽象（代理，描述），就像操作一个本地的集合一样
    //切分压平
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词和1组合在一起
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    //聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    //可以写入外部系统
    //在这里，我来打印结果(Action)
    reduced.print()

    //启动sparksteaming程序
    ssc.start()
    //等待优雅慢慢的退出
    ssc.awaitTermination()


  }
}
