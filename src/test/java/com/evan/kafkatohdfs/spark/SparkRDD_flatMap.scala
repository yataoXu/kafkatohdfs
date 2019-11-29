package com.evan.kafkatohdfs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD_flatMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("firsRDD").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    var List_RDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))
    var flatMap_RDD: RDD[Int] = List_RDD.flatMap(datas => datas)
    flatMap_RDD.collect().foreach(println)
  }
}
