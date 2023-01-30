package com.yee.study.bigdata.scala.grammar

/**
 * 控制语法示例
 *
 * @author Roger.Yi
 */
object ControlSample {

  def main(args: Array[String]): Unit = {
    val x = 10
    val s = if (x > 0) 1 else -1
    val s2 = if (x > 0) 1 else () // () 就是 Unit

    var n = 6
    var r = 1
    while (n > 0) {
      r = r * n
      n -= 1
    }

    // 14 15 24 25 34 35
    for (i <- 1 to 3; j <- 4 to 5) print(f"${10 * i + j}%3d")
    println()

    // 13 22 23 31 32 33
    for (i <- 1 to 3; from = 4 - i; j <- from to 3) print(f"${10 * i + j}%3d")
    println()

    // Vector(H, e, l, l, o, I, f, m, m, p)
    val vector = for (i <- 0 to 1; c <- "Hello") yield (c + i).toChar
    println(vector)
  }
}
