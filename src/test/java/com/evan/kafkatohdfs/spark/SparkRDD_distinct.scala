package com.evan.kafkatohdfs.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkRDD_distinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("distinct").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    var List_RDD: RDD[Int] = sc.makeRDD(List(2,2,3,4,3,4,5,6))
    // 对RDD进行去重，不指定并行度
    var distinctRDD: RDD[Int] = List_RDD.distinct()
    distinctRDD.collect().foreach(println)

    // 对RDD进行去重，指定并行度
    var distinctRDD1: RDD[Int] = List_RDD.distinct(2)
    distinctRDD1.collect().foreach(println)
  }
}
