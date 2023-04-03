package com.yee.study.bigdata.scala.grammar

/**
 * 隐式参数示例
 *
 * @author Roger.Yi
 */
object ImplicitSample {

  def main(args: Array[String]): Unit = {
    implicitCall()

    implicitClass()

    implicitlyCall()
  }

  /**
   * Here, we define an implicit value greeting of type String.
   * If we have a method or function that takes a String parameter and
   * we call it without passing an argument, the compiler will look for
   * an implicit value of type String and find the greeting value.
   */
  def implicitCall(): Unit = {
    implicit val greeting: String = "Hello, world!"
    greet("Alice")
  }

  /**
   * 当编译器看到 3 x 4 的时候，首先发现 3 这个 Int 没有 x 函数，然后就找隐式转换，
   * 通过上面的隐式转换函数生成了一个RectangleMaker(3)，然后调用RectangleMaker的 x 函数，
   * 就产生了一个矩形Rectangle(3,4)
   */
  def implicitClass(): Unit = {
    val easyRec = 3 x 4
    println(easyRec) // Rectangle(3,4)
  }

  def implicitlyCall(): Unit = {
    implicit val personOrdering: Ordering[Person] = Ordering.by(_.age)

    val people = List(Person("Alice", 25), Person("Bob", 20), Person("Charlie", 30))
    val sortedPeople = sortPeople(people)(implicitly[Ordering[Person]])
    println(sortedPeople)
  }

  def greet(name: String)(implicit greeting: String): Unit = {
    println(s"$greeting $name")
  }

  case class Rectangle(width: Int, height: Int)

  implicit class RectangleMaker(width: Int) {
    def x(height: Int) = Rectangle(width, height)

    def *(height: Int) = Rectangle(width, height)
  }

  case class Person(name: String, age: Int)

  def sortPeople(people: List[Person])(implicit ord: Ordering[Person]): List[Person] = {
    people.sorted
  }
}
