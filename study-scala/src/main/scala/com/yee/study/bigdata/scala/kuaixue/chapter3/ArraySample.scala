package com.yee.study.bigdata.scala.kuaixue.chapter3

import scala.collection.mutable.ArrayBuffer

/**
 *
 * @author Roger.Yi
 */
object ArraySample {

  def main(args: Array[String]): Unit = {
    // 定长数组
    val nums = new Array[Int](10)
    val s = Array("Hello", "World")
    s(0) = "Goodbye"
    println(s)

    // 变长数组: ArrayBuffer
    val b = ArrayBuffer[Int]()
    b += 1 // (1)
    b += (1, 2, 3, 5) // (1, 1, 2, 3, 5)
    b ++= Array(8, 13, 21) // (1, 1, 2, 3, 5, 8, 13, 21)
    b.trimEnd(5) // (1, 1, 2)

    b.insert(2, 6) // (1, 1, 6, 2)
    b.insert(2, 6, 7)
    b.insert(2, 7, 8, 9) // (1, 1, 7, 8 ,9, 6, 2)
    b.remove(2) // (1, 1, 8, 9, 6, 2)
    b.remove(2, 3) // （1, 1, 2)
    val a = b.toArray
    println(a.toList)
  }
}
