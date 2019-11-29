package com.evan.kafkatohdfs.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object StreamingByHdfs {

  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.监控文件夹创建DStream
    val dirStream = ssc.textFileStream("hdfs://10.2.196.18:9000/fileStream")

    //4.将每一行数据做切分，形成一个个单词
    val wordStreams = dirStream.flatMap(_.split("\t"))

    //5.将单词映射成元组（word,-
    val wordAndOneStreams = wordStreams.map((_, 1))

    //6.将相同的单词次数做统计
    val wordAndCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_ + _)

    //7.打印
    wordAndCountStreams.print()

    //8.启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
