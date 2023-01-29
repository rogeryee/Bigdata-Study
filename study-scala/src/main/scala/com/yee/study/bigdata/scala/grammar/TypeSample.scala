package com.yee.study.bigdata.scala.grammar

import scala.reflect.runtime.universe._

/**
 * 类型结构示例
 *
 * @author Roger.Yi
 */
object TypeSample {

  def main(args: Array[String]): Unit = {
    // 类型与类
    typeAndClass()

    // 结构类型
    structuralType()
  }

  // 类型与类
  def typeAndClass(): Unit = {
    // 获取类型信息
    // com.yee.study.bigdata.scala.grammar.TypeSample.A
    println(typeOf[A])

    // class com.yee.study.bigdata.scala.grammar.TypeSample$A
    println(classOf[A])

    // class com.yee.study.bigdata.scala.grammar.TypeSample$A
    val a = new A
    println(a.getClass)

    // com.yee.study.bigdata.scala.grammar.TypeSample.T
    println(typeOf[T])

    // interface com.yee.study.bigdata.scala.grammar.TypeSample$T
    println(classOf[T])

    // typeOf 和 classOf 方法接收的都是类型符号(symbol)，并不是对象实例
    // not found: type O
    // println(classOf[O])

    // 对于实例，要获取他的 Class 信息，只有通过 getClass 方法
    // class com.yee.study.bigdata.scala.grammar.TypeSample$O$
    println(O.getClass)

    // 单例，它的类型与它的类不同，要用 A.type 来表示
    // com.yee.study.bigdata.scala.grammar.TypeSample.O.type
    println(typeOf[O.type])

    // 内部类
    val a1 = new A
    val a2 = new A
    val b1 = new a1.B
    val b2 = new a2.B

    // class com.yee.study.bigdata.scala.grammar.TypeSample$A$B
    println(b1.getClass)

    // b1、b2 class都是相同的: A$B
    println(b1.getClass == b2.getClass) // true

    // b1、b2 类型却是不同的
    println(typeOf[a1.B] == typeOf[a2.B]) // false

    println(typeOf[a1.B] <:< typeOf[A#B]) // true
    println(typeOf[a2.B] <:< typeOf[A#B]) // true

    // a1.foo方法接受的参数类型为：a1.B，而传入的b2 类型是 a2.B，两者不匹配。
    // typeOf[a1.B]、typeOf[a2.B] 不相等
    // def foo(b: B) 相当于 this.B 或 A.this.B
    // type mismatch;
    // found   : a2.B
    // required: a1.B
    // a1.foo(b2)

    // 类型投影(type projection)
    // 表达所有的外部类A实例路径下的B类型，即对 a1.B 和 a2.B及所有的 an.B类型找一个共同的父类型，这就是类型投影，用 A#B的形式表示
    // com.yee.study.bigdata.scala.grammar.TypeSample$A$B@56781d96
    a1.foo2(b2)
  }

  // 结构类型
  def structuralType(): Unit = {
    // Closed
    free(new { def close() = println("Closed") })

    // Closed.free2
    free2(new { def close()=println("Closed.free2") })
  }

  class A {

    class B

    // 相当于 this.B 或 A.this.B
    def foo(b: B) = println(b)

    def foo2(b: A#B) = println(b)
  }

  trait T

  object O

  // 参数中包含结构中声明的方法或值即可
  def free(res: { def close(): Unit }): Unit = {
    res.close()
  }

  // 通过type在定义类型时，将其声明为结构类型
  type X = { def close(): Unit }
  def free2(res: X) = res.close()
}
