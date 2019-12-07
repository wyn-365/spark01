package com.wang.spark.sql2

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by zx on 2017/5/13.
  */
object JdbcDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //load这个方法会读取真正mysql的数据吗？
    val logs: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/bigdata_log",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "logs",
        "user" -> "root",
        "password" -> "123456")
    ).load()

    //logs.printSchema()


    //logs.show()

//    val filtered: Dataset[Row] = logs.filter(r => {
//      r.getAs[Int]("age") <= 13
//    })
//    filtered.show()

    //lambda表达式 先过滤  这样处理的数据在内存中较为少  比较快
    val r = logs.filter($"age" <= 13)

    //val r = logs.where($"age" <= 13)

    val reslut: DataFrame = r.select($"id", $"name", $"age" * 10 as "age")

    //把数据写入到数据库中，并且自动建表    //不要提前建立数据表  程序自己建立，否则会出错
    val props = new Properties()
    props.put("user","root")
    props.put("password","123456")
    reslut.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata_log", "logs1", props)

    //DataFrame保存成text时出错(只能保存一列) 下面都是自动创建的文件夹

    reslut.write.text("/Users/zx/Desktop/text") //文本

    reslut.write.json("/Users/zx/Desktop/json") //json文件

    reslut.write.csv("/Users/zx/Desktop/csv") //分隔符为逗号

    reslut.write.parquet("hdfs://hadoop1:9000/parquet") //智能读取 哈哈哈哈这个厉害！！！！！


    reslut.show()

    spark.close()


  }
}
