package com.evan.kafkatohdfs.spark

import org.apache.spark.{SparkConf, SparkContext}
object RemoteDemo {
  def main(args: Array[String]): Unit = {
    println(123)
    val conf = new SparkConf().setMaster("spark://10.2.196.21:7077").setAppName("firstapp")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List(1, 2, 3, 4))
    a.persist();
    println(a.count())
    println("============================")
    a.collect().foreach(println)
  }
}



