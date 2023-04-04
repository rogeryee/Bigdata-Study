package com.yee.study.bigdata.scala.grammar

/**
 * 泛型示例
 *
 * @author Roger.Yi
 */
object GenericSample {

  def main(args: Array[String]): Unit = {
    genericClass()

    funcWithUpperBound(Array(new Person))
    funcWithUpperBound(Array(new Student))
    // 编译出错，必须是Person的子类
    // funcWithUpperBound(Array("Scala"))

    funcWithLowerBound(Array(new Person))
    funcWithLowerBound(Array(new Policeman))
    // 编译出错，必须是Policeman的子类
    // funcWithLowerBound(Array(new Superman))

    funcWithUpperAndLowerBound(Array(new Person))
    funcWithUpperAndLowerBound(Array(new Policeman))
    // 编译出错：Superman是Policeman的子类
    // funcWithUpperAndLowerBound(Array(new Superman))
    // 编译出错：Human是Person的父类
    // funcWithUpperAndLowerBound(Array(new Human))

    // 非变
    val A: Temp1[Sub] = new Temp1[Sub]
    // 编译出错，非变类型
    // val B: Temp1[Super] = A

    // 协变
    val C: Temp2[Sub] = new Temp2[Sub]
    val D: Temp2[Super] = C

    // 逆变
    val E: Temp3[Super] = new Temp3[Super]
    val F: Temp3[Sub] = E
  }

  /**
   * 泛型
   */
  def genericClass(): Unit = {
    println(Pair("Scala", "Hadoop"))
    println(Pair("Scala", 2.13))
  }

  /**
   * 上界
   */
  def funcWithUpperBound[T <: Person](a: Array[T]) = println(a)

  /**
   * 下界
   */
  def funcWithLowerBound[T >: Policeman](a: Array[T]) = println(a)

  /**
   * 上下界
   */
  def funcWithUpperAndLowerBound[T >: Policeman <: Person](array: Array[T]) = println(array)

  case class Pair[T](var a: T, var b: T)

  class Human

  class Person extends Human

  class Student extends Person

  class Policeman extends Person

  class Superman extends Policeman

  class Super

  class Sub extends Super

  // 非变
  class Temp1[T]

  // 协变
  class Temp2[+T]

  // 逆变
  class Temp3[-T]
}
