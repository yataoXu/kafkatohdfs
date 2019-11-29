package com.evan.kafkatohdfs.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkRDD_coalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("repartition").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val source: RDD[Int] = sc.makeRDD(1 to 10,4)
    println("缩减分区前的分区数："+ source.partitions.size)
    val repartitionRDD: RDD[Int] = source.repartition(2)
   println("缩减分区后的分区数："+ repartitionRDD.partitions.size)
  }
}

