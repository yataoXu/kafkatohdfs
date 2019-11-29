package com.evan.kafkatohdfs.deltaLake

import org.apache.spark.{SparkConf, SparkContext}
import scala.math.random

object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://10.2.196.18:7077").setJars(Seq("D:\\SparkExample.jar"))
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    println("Time:" + spark.startTime)
    val n = math.min(1000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}