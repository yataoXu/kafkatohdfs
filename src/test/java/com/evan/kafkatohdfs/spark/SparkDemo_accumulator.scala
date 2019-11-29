package com.evan.kafkatohdfs.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object SparkDemo_accumulator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    val sc: SparkContext = new SparkContext(conf)

    var dataRDD: RDD[Int] = sc.makeRDD(List(2, 2, 3, 4, 3, 4, 5, 6), 2)

    // 方法一
    val sumRDD: Int = dataRDD.reduce(_ + _)
    println(sumRDD)

    //虽然方法一看上去很简单，不过他需要两个Executor将dataRDD中的两个分区的数据先累加起来，然后得到分区数据的和之后，再把这两个和相加得到最终数据

    println("__________________________")
    // 方法二
    var sum = 0;
    dataRDD.foreach(i => sum += i)
    println(sum) // 0
    // 最终得到方法二的sum 为0


    // 使用累加器共享变量

    // 创建累加器共享变量
    val accumulator: LongAccumulator = sc.longAccumulator
    dataRDD.foreach {
      case i => {
        // 执行累加器的累加功能
        accumulator.add(i)
      }
    }
    // 获取累加器的值
    println(accumulator.value)


  }
}
