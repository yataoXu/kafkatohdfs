package com.evan.kafkatohdfs.spark

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlRDD {
  def main(args: Array[String]): Unit = {
    // 1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    // 2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    // 3.定义连接mysql的参数
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://10.2.196.18:3306/evan"
    val userName = "root"
    val passWd = "Qwe123!@#"
    //创建JdbcRDD
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select age,name from `test` where `id`>=? and id <=?",
      1,
      3,
      2,
      r => (r.getInt(1), r.getString(2))
    )

    //打印最后结果
    println("____________________")
    println(rdd.count())
    rdd.foreach(println)

    sc.stop()
  }
}
