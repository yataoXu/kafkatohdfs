package com.evan.kafkatohdfs.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object StreamingByFileData {
  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf = {
      new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    }


    // 2.初始化SparkStreamingContext
    // 实时解析环境对象
    // 采集周期：以指定的时间为周期实时采集数据
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 3.从指定文件夹中采集数据
    val fileDataStreams: DStream[String] = ssc.textFileStream("streamfile")

    // 将采集的数据进行分解（扁平化）
    val wordStreams = fileDataStreams.flatMap(_.split(" "))

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
