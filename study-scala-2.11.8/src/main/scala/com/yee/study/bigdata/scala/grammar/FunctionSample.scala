package com.yee.study.bigdata.scala.grammar

/**
 * 函数示例
 *
 * @author Roger.Yi
 */
object FunctionSample {

  def main(args: Array[String]): Unit = {
    def abs(x: Double) = if (x > 0) x else -x

    def fac(n: Int) = {
      var r = 1
      for (i <- 1 to n) r = r * i
      r
    }

    // 默认参数、带名参数
    def decorate(str: String, left: String = "[", right: String = "]") = left + str + right

    println(decorate("hello"))
    println(decorate("hello", "<<<", ">>>"))
    println(decorate("hello", right = "]###"))

    // 变长参数
    def sum(args: Int*) = {
      var result = 0
      for (arg <- args) result += arg
      result
    }

    println(sum(1, 2, 3))
    // println(sum(1 to 5)) 错误 值序列 不能传入
    println(sum(1 to 5: _*)) // 将值序列作为参数处理

    def recursiveSum(args: Int*): Int = {
      if (args.length == 0) 0
      else args.head + recursiveSum(args.tail: _*)
    }

    // 柯里化函数
    val f = (x : Double) => Math.PI / 2 - x
    val cos = f andThen math.sin

    println(cos(1))
  }
}
