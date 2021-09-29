package com.yee.study.bigdata.scala.util

import scala.collection.TraversableOnce

/**
 *
 * @author Roger.Yi
 */
object PrintUtil {

  def _print(prefix: String, obj: Any): Unit = {
    printImp(Option(prefix), obj)
  }

  def _print(obj: Any): Unit = {
    printImp(None, obj)
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
}
