package com.wang.spark.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaLambdaWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWordCount");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定数据读取目录
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分
        //Iterator<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //将单词和1组合
        //words.mapToPair(w -> new Tuple2<>(w,1)).var



    }



}
