package com.evan.kafkatohdfs.spark

object demo {
  def main(args: Array[String]): Unit = {
    def f(): Unit = {
      println("function")
    }

      //    def f0() = {
      //
      //      // 返回函数f
      //      // 直接返回函数，需要加上特殊符号：_
      //      f _
      //    }
      //
      //    f0()


      def f1(i: Int) = {
        def f2(j: Int): Int = {
          i * j
        }

        f2 _
      }

      val intToInt: (Int) => Int = f1(2)
      // f1的返回值:入参为Int 返回值为Int类型的函数


      // 将函数作为参数传递给另一个函数，需要采用特殊的声明方式:()=>Unit
      // 参数列表 => 返回值类型
      def f4(f : () => Int): Int = {
        f() + 10
      }
      def f5(): Int = {
        5
      }
      println(f4(f5))

    }

  // 匿名函数
  def f6(f:() => Unit) :Unit ={
    f()
  }

  f6(()=>{
    println("xxxxx")
  })

}

//    def f3(i:Int)(j:Int) :Int ={
//      i*j
//    }
//
//    println(f3(2)(3))

