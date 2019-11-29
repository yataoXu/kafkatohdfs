package com.evan.kafkatohdfs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCout {
  def main(args: Array[String]): Unit = {
    // local 模式 loal local[] local[*]
    val config: SparkConf =  new SparkConf().setMaster("local").setAppName("firstDemo")
    // 创建Spark上下文对象
    val sc = new SparkContext(config)
    // 设置检查点的保存目录
    sc.setCheckpointDir("hdfs://10.2.196.18:9000/checkpoint")
    //读取文件
    val lines:RDD[String] = sc.textFile("D:\\people.txt")
    // 将一行一行的数据分解成一个一个的单词
    val words:RDD[String] = lines.flatMap(_.split(" "))
    // 为了方便统计，将单词数据进行结构化转换
    val wordToOne:RDD[(String,Int)] = words.map((_,1))
    wordToOne.checkpoint()
    // 对转换结构后的输几局进行分组聚合
    val wordToSum: RDD[(String,Int)] = wordToOne.reduceByKey(_+_)


    //println
    println(wordToOne.toDebugString)

    println("-------------")

    println(wordToSum.toDebugString)
    println("-------------")
    wordToSum.foreach(println)
    println("-------------")
  }
}

