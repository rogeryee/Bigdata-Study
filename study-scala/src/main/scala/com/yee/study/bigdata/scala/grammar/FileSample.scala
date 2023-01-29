package com.yee.study.bigdata.scala.grammar

import scala.io.Source

/**
 *
 * @author Roger.Yi
 */
object FileSample {

  def main(args: Array[String]): Unit = {

    def widthOfLength(s: String) = s.length.toString.length

    val file = "/etc/hosts"
    val lines = Source.fromFile(file).getLines.toList
    val longestLine = lines.reduceLeft(
      (a, b) => if (a.length > b.length) a else b
    )
    val maxWidth = widthOfLength(longestLine)
    for (line <- lines) {
      val numSpaces = maxWidth - widthOfLength(line)
      val padding = " " * numSpaces
      println(padding + line.length + " | " + line)
    }
  }
}
