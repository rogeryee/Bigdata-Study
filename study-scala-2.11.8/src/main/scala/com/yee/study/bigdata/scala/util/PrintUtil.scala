package com.yee.study.bigdata.scala.util

import scala.collection.TraversableOnce

/**
 *
 * @author Roger.Yi
 */
object PrintUtil {

  def _print(obj: Any): Unit = {
    printImp(None, obj)
  }

  def _print_(obj: Any)(prefix: String): Unit = {
    printImp(Option(prefix), obj)
  }

  private def printImp(prefix: Option[String], obj: Any): Unit = {
    val msg =
      obj match {
        case t: TraversableOnce[_] => obj.asInstanceOf[TraversableOnce[_]].mkString(", ")
        case _ => obj.toString
      }

    val prefixStr = prefix match {
      case Some(s) if (!s.isEmpty) => s + " : "
      case _ => ""
    }

    println(prefixStr + msg)
  }

  def main(args: Array[String]): Unit = {
    _print_("abc", "cde")("Pre")
    _print_(1, 2, 3)("Pre")
    _print(1, 2, 3)
    _print_(1, 2, 3)("Pre")
  }
}
