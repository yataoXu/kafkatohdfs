package com.evan.kafkatohdfs.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(1,2,3))
    println("---------------")
    println(rdd1.reduce(_+_))
    println("---------------")
  }
}



