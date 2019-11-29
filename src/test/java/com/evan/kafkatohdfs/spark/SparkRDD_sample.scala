package com.evan.kafkatohdfs.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkRDD_sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sample")
    val sc: SparkContext = new SparkContext(conf)
    val source: RDD[Int] = sc.makeRDD(1 to 10)
    val sampleRDD: RDD[Int] = source.sample(true,4,1)
    sampleRDD.collect().foreach(println)
  }
}
