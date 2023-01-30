package com.yee.study.bigdata.scala.grammar

import scala.collection.mutable.ArrayBuffer

/**
 * 数组示例
 *
 * @author Roger.Yi
 */
object ArraySample {

  def main(args: Array[String]): Unit = {
    // 定长数组
    val nums = new Array[Int](10)
    val s = Array("Hello", "World")
    s(0) = "Goodbye"
    println(s.toList)

    // 变长数组: ArrayBuffer
    val b = ArrayBuffer[Int]()
    b += 1 // (1)
    b += (1, 2, 3, 5) // (1, 1, 2, 3, 5)
    b ++= Array(8, 13, 21) // (1, 1, 2, 3, 5, 8, 13, 21)
    b.trimEnd(5) // (1, 1, 2)

    b.insert(2, 6) // (1, 1, 6, 2)
    b.insert(2, 7, 8, 9) // (1, 1, 7, 8 ,9, 6, 2)
    b.remove(2) // (1, 1, 8, 9, 6, 2)
    b.remove(2, 3) // （1, 1, 2)
    println(b.toList)

    // 遍历
    val a = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    for (i <- 0 until a.length)
      println(s"$i: ${a(i)}")

    for (i <- 0 until a.length by 2)
      println(s"$i: ${a(i)}")

    for (i <- a.indices)
      println(s"$i: ${a(i)}")

    for (i <- a)
      println(s"$i")

    // 数组转化
    val a2 = Array(2, 3, 5, 7, 11)
    val result = for (elem <- a2)
      yield 2 * elem

    println(result.toList)

    // 常用方法
    val a3 = Array(1, 7, 2, 9)
    println(a3.sum)
    println(a3.max)
    val a3Sorted = a3.sorted
    val a3Desc = a3.sortWith(_ > _)
    println(a3Sorted.toList)
    println(a3Desc.toList)

    println(a.mkString(" and "))
    println(a.mkString("<<", " and ", ">>"))

    // 多维
    val matrix = Array.ofDim[Double](3, 4) // 三行四列
    val triangle = new Array[Array[Int]](10)
  }
}
