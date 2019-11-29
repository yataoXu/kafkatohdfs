package com.evan.kafkatohdfs.spark

object Function_demo {
  def main(args: Array[String]): Unit = {
    //面向对象：解决问题的时候，将问题拆分成一个一个小问题，分别解决
    //关心的是对象的关系： 继承，实现，重写，多态

    // 函数式编程：关心的是问题的解决方案（封装功能）。面向解题过程
    // 重点在于函数（功能）以及入参和出参。


    //    public static void test(String s){
    //      //方法体
    //      System.out.println(s)
    //    }
    //
    //    def test(s: String):Unit = {
    //      println(s)
    //    }
    //
    //
    //    test("evan")
    def mysqlConect(url: String ="jdbc: mysql://10.2.196.18/evan",
                    port: Int = 3306,
                    user: String ="root",
                    pwd:String ="root"): Unit = {
      println(s"url=${url}")
    }


    def f(p1:String ="v1",p2:String): Unit ={
      println(p1+p2)
    }
    f(p2 = "v2" )

  }
}


