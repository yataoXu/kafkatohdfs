package com.evan.kafkatohdfs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD_glom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("glomFunc")
    val sc: SparkContext = new SparkContext(conf)
    val source: RDD[Int] = sc.makeRDD(List(1, 2, 3, 5, 6, 7, 8, 9))
    val glomRDD: RDD[Array[Int]] = source.glom()
    glomRDD.collect().foreach(array => {
      println(array.mkString(","))
    })
  }
}
