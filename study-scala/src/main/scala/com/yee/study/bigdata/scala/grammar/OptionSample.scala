package com.yee.study.bigdata.scala.grammar

/**
 *
 * @author Roger.Yi
 */
object OptionSample {

  def main(args: Array[String]): Unit = {
    println(parseString2Int("100").get)
    println(parseString2Int("hello").getOrElse("NAN"))
  }

  def parseString2Int(num: String): Option[Int] = {
    val result: Option[Int] = Try(num.toInt)
    result
  }

  def Try[A](a: => A): Option[A] = {
    try Some(a)
    catch {
      case e: Exception => None
    }
  }
}
