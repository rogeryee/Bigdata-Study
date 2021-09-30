package com.yee.study.bigdata.scala.kuaixue.chapter4

import com.yee.study.bigdata.scala.util.PrintUtil._print

/**
 *
 * @author Roger.Yi
 */
object TupleSample {

  def main(args: Array[String]): Unit = {
    val t = (1, 3.14, "Fred")
    _print(t._1)
    _print(t._2)
    _print(t _2)

    val (first, second, third) = t
    _print(first, second, third)

    val (first2, _, third2) = t
    _print(first2, third2)

    // 拉链操作
    val symbols = Array("<", "=", ">")
    val counts = Array(2, 10, 2)
    val pairs = symbols.zip(counts)
    _print(pairs.toList)

    for((s, n) <- pairs) print(s * n)
  }
}
