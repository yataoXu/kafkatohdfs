package com.evan.kafkatohdfs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD_Serializable {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    // 创建spark上下文对象
    val sc = new SparkContext(sparkConf)

    // 2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "flink"))

    // 3.创建一个Search对象
    val search = new Search("h")

    // 4.运用第一个过滤函数并打印结果
    val match1: RDD[String] = search.getMatch2(rdd)
    match1.collect().foreach(println)
    // 释放资源
    sc.stop()
  }
}

class Search(query: String) {

  // 过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  // 过滤出包含字符串的RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  // 过滤出包含字符串的RDD
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }

}