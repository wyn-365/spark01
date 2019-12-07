package com.wang.spark.hiveonspark

import org.apache.spark.sql.SparkSession

/**
 * spark整合hive
 */
object HiveOnSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HiveOnSpark")
      .master("local[*]")
      //如果想让hive运行在spark上面，一定要开启spark对hive的支持,兼容hive的语法
      .enableHiveSupport()
      .getOrCreate()


    //想要使用hive的元数据库，必须指定hive源数据库的位置，
    // 添加hive-site.xml 到classpath到当前程序就可了




    //查询
    val result = spark.sql("select * from t_boy")
    result.show()
    spark.close()
  }
}
