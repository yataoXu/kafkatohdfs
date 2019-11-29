package com.evan.kafkatohdfs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("firsRDD").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val source: RDD[Int] = sc.makeRDD(1 to 10)
    val i = 10
    // 所有RDD算子的计算功能由Executor执行
    val doubleNum: RDD[Int] = source.map(_ * i)
    doubleNum.foreach(println)
  }
}
