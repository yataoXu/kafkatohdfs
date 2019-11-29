package com.evan.kafkatohdfs.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object SliderDemo {
  def main(args: Array[String]): Unit = {

    // scala 中的窗口的概念
    val ints = List(1,2,3,4,5,6)
    // 滑动窗口
    val intsSliding = ints.sliding(3,3)
    for(list<-intsSliding){
      println(list.mkString(","))

    }

    // spark 窗口的概念
    //1.创建SparkConf并初始化SSC
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("window")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //2.定义kafka参数
    val brokers = "10.2.196.18:9092,10.2.196.19:9092,10.2.196.20:9092"
    val topic = "example"
    val consumerGroup = "spark"


    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      brokers,
      topic,
      Map(topic -> 1)
    )

    /**
      * window(windowDuration: Duration, slideDuration: Duration)
      * 第一个参数：窗口大小
      * 第二个参数：滑动步长
      * 两个参数都必须为采集周期的整数倍
      */
    kafkaDStream.window(Seconds(9),Seconds(3))
    // 将采集的数据进行分解（扁平化）
    val wordStreams: DStream[String] = kafkaDStream.flatMap(t =>t._2.split(" "))

    // 将数据进行结构的转换方便统计分解
    val wordAndOneStreams: DStream[(String, Int)] = wordStreams.map((_, 1))

    // 将相同的单词次数做聚合处理
    val wordAndCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_+_)

    // 打印
    println(">>>>>>>>>")
    wordAndCountStreams.print()

    // 启动SparkStreamingContext
    ssc.start()
    // Driver 等待采集器的执行
    ssc.awaitTermination()
  }
}
