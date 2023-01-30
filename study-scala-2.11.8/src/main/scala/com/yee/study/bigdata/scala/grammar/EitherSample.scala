package com.yee.study.bigdata.scala.grammar

import java.net.URL
import scala.io.Source

/**
 * Either 示例
 *
 * sealed trait Either[+E, +A]
 * case class Left[+E](value: E) extends Either[E, Nothing]
 * case class Right[+A](value: A) extends Either[Nothing, A]
 *
 * Either 也是一个容器类型，但不同于 Try、Option，它需要两个类型参数： Either[A, B] 要么包含一个类型为 A 的实例，要么包含一个类型为 B 的实例
 * Either 只有两个子类型： Left、 Right， 如果 Either[A, B] 对象包含的是 A 的实例，那它就是 Left 实例，否则就是 Right 实例。
 * 按照约定，处理异常时，Left 代表出错的情况，Right 代表成功的情况。
 *
 * @author Roger.Yi
 */
object EitherSample {

  def main(args: Array[String]): Unit = {
    printURL(getContent(new URL("http://www.baidu.com")))
    printURL(getContent(new URL("http://mail.google.com")))
  }

  def printURL(url: Either[String, Source]) = {
    // 调用 isLeft （或 isRight ）方法询问一个 Either，判断它是 Left 值，还是 Right 值
    // 也可以使用模式匹配
    url match {
      case Left(msg) => println(msg)
      case Right(source) => source.getLines().foreach(println)
    }
  }

  def getContent(url: URL): Either[String, Source] = {
    if(url.getHost.contains("google")) {
      Left("Requested URL is blocked")
    } else {
      Right(Source.fromURL(url))
    }
  }
}
