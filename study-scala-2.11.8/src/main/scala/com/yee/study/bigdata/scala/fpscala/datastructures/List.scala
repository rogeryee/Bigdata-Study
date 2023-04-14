package com.yee.study.bigdata.scala.fpscala.datastructures

/**
 * 单项链表
 *
 * @author Roger.Yi
 */
sealed trait List[+A]

case object Nil extends List[Nothing]

case class Cons[+A](head: A, tail: List[A]) extends List[A]

object MyApp {

  def main(args: Array[String]): Unit = {
    val list = List("a", "b")
    println(list) // Cons(a, Cons(b, Nil))
  }
}

object List {

  def sum(ints: List[Int]): Int = ints match {
    case Nil => 0
    case Cons(x, xs) => x + sum(xs)
  }

  def product(ds: List[Double]): Double = ds match {
    case Nil => 1.0
    case Cons(0.0, _) => 0.0
    case Cons(x, xs) => x * product(xs)
  }

  def apply[A](as: A*): List[A] = {
    if (as.isEmpty) Nil
    else Cons(as.head, apply(as.tail: _*))
  }
}
