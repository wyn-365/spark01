package com.wang.spark.sql2

import com.wang.ip.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**注意：：：：：：：：
  * 如果数据量非常的大  这个例题不使用
 * 因为join会浪费太大的代价  会很慢！！！！！！！！！！！！！！！！！！！
 * 如何优化join呢？？？？？？？
  */
object IpLoactionSQL {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("JoinTest")
      .master("local[*]")
      .getOrCreate()

    //取到HDFS中的ip规则
    import spark.implicits._ //导入隐士转换 得到的实例
    val rulesLines:Dataset[String] = spark.read.textFile(args(0))

    //整理ip规则数据()
    val ruleDataFrame: DataFrame = rulesLines.map(line => {
      //  1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province") //放到元祖里面 为他们取名


    //创建RDD，读取访问日志
    val accessLines: Dataset[String] = spark.read.textFile(args(1))

    //整理数据
    val ipDataFrame: DataFrame = accessLines.map(log => {
      //  20090121000132095572000|125.213.100.123|show.51.com|/shoplist.php?phpfile=shoplist2.php&style=1&sex=137|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; Mozilla/4.0(Compatible Mozilla/4.0(Compatible-EmbeddedWB 14.59 http://bsalsa.com/ EmbeddedWB- 14.59  from: http://bsalsa.com/ )|http://show.51.com/main.php|
      //将log日志的每一行进行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      //将ip转换成十进制
      val ipNum = MyUtils.ip2Long(ip)//转换成十进制
      ipNum
    }).toDF("ip_num")

    //创建视图、注册视图
    ruleDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")

    //关联join
    val r = spark.sql("SELECT province, count(*) counts FROM v_ips JOIN v_rules ON (ip_num >= snum AND ip_num <= enum) GROUP BY province ORDER BY counts DESC")

    r.show()

    spark.stop()



  }
}
