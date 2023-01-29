package com.yee.study.bigdata.scala.grammar

/**
 * 型变(Variance) 示例
 *
 * 1. 术语表
 * Variance	      型变	    Function[-T, +R]
 * Nonvariant	    不变	    Array[A]
 * Covariant	    协变	    Supplier[+A]
 * Contravariant	逆变  	  Consumer[-A]
 * Immutable	    不可变的	String
 * Mutable	      可变的	  StringBuilder
 *
 * 2. 形式化
 * 型变(Variance)」拥有三种基本形态：协变(Covariant), 逆变(Contravariant), 不变(Nonconviant)，可以形式化地描述为：
 * 一般地，假设类型C[T]持有类型参数T；给定两个类型A和B，如果满足A <: B，则C[A]与 C[B]之间存在三种关系：
 * 如果C[A] <: C[B]，那么C是协变的(Covariant);
 * 如果C[A] :> C[B]，那么C是逆变的(Contravariant);
 * 否则，C是不变的(Nonvariant)。
 *
 * 3. Scala 表示
 * Scala的类型参数使用+标识「协变」，-标识「逆变」，而不带任何标识的表示「不变」
 *
 * @author Roger.Yi
 */
object VarianceSample {

  class Animal {
    def eat(): Unit = {
      println("动物要吃食物")
    }
  }

  class Cat extends Animal {
    override def eat(): Unit = println("猫吃鱼")
  }

  class Tiger extends Cat {
    override def eat(): Unit = println("老虎吃肉")
  }

  class Nonvariant[T] {
  }

  class Covariant[+T] {
  }

  class Contravariant[-T] {
  }

  def main(args: Array[String]): Unit = {
    var cat = new Cat
    var tiger = new Tiger

    val cat2: Cat = tiger

    val invCat: Nonvariant[Cat] = new Nonvariant[Cat]
    val invTiger: Nonvariant[Tiger] = new Nonvariant[Tiger]
    //    val invCat2: Nonvariant[Cat] = invTiger // error: type mismatched
    //    val invTiger2: Nonvariant[Tiger] = invCat // error: type mismatched

    val covCat: Covariant[Cat] = new Covariant[Cat]
    val covTiger: Covariant[Tiger] = new Covariant[Tiger]
    val covCat2: Covariant[Cat] = covTiger
    //    val covTiger2: Covariant[Tiger] = covCat // error: type mismatched

    val inverCat: Contravariant[Cat] = new Contravariant[Cat]
    val inverTiger: Contravariant[Tiger] = new Contravariant[Tiger]
    //    val inverCat2: Inversion[Cat] = inverTiger // error: type mismatched
    val inverTiger2: Contravariant[Tiger] = inverCat
  }
}
