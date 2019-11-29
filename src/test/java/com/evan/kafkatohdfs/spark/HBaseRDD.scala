//package com.evan.kafkatohdfs.spark
//
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//object HBaseRDD {
//  def main(args: Array[String]): Unit = {
//    //创建spark配置信息
//    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseRDD")
//
//    //创建SparkContext
//    val sc = new SparkContext(sparkConf)
//
//    //构建HBase配置信息
//    val conf: Configuration = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "10.2.196.18:2181,10.2.196.21:2181,10.2.196.20:2181")
//    conf.set(TableInputFormat.INPUT_TABLE, "test")
//
//    //从HBase读取数据形成RDD
//    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
//      conf,
//      classOf[TableInputFormat],
//      classOf[ImmutableBytesWritable],
//      classOf[Result])
//
//    val count: Long = hbaseRDD.count()
//    println(count)
//
//    //对hbaseRDD进行处理
//    hbaseRDD.foreach {
//      case (rowKey, result) => {
//        val cells: Array[Cell] = result.rawCells()
//        for (cell <- cells) {
//          println(Bytes.toString(CellUtil.cloneValue(cell)))
//        }
//      }
//    }
//
//    val value = sc.makeRDD(List(("zhangsan",20), ("Lisi",20), ("Wangwu",30)))
//
//        val key: String = Bytes.toString(result.getRow)
//        val name: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
//        val color: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("color")))
//        println("RowKey:" + key + ",Name:" + name + ",Color:" + color)
//    }
//
//    //关闭连接
//    sc.stop()
//  }
//
//}
