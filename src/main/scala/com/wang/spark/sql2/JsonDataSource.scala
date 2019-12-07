package com.wang.spark.sql2

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 读取json数据源
  */
object JsonDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //指定以后读取json类型的数据(有表头)
    val jsons: DataFrame = spark.read.json("/Users/zx/Desktop/json")

    val filtered: DataFrame = jsons.where($"age" <=500)


    filtered.printSchema()

    filtered.show()

    spark.stop()


  }
}
