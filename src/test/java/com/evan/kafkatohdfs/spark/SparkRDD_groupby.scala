package com.evan.kafkatohdfs.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkRDD_groupby {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("glomFunc")
    val sc: SparkContext = new SparkContext(conf)
    val source: RDD[Int] = sc.makeRDD(1 to 10)
    val groubyRDD: RDD[(Int, Iterable[Int])] = source.groupBy(_ % 3)
    groubyRDD.collect().foreach(println)
  }
}
