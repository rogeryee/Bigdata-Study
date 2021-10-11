package com.yee.study.bigdata.scala.kuaixue.chapter5

import com.yee.study.bigdata.scala.util.PrintUtil._print

/**
 *
 * @author Roger.Yi
 */
object ClassSample {

  def main(args: Array[String]): Unit = {
    val myCounter = new Counter // 同样 new Counter()
    myCounter.increment()
    _print(myCounter.current) // 同样 myCounter.current()

    val person = new Person
    person.age_=(21) // setter
    _print(person.age) // getter
  }

  class Counter {
    private var value = 0

    def increment() {
      value += 1
    }

    def current() = value
  }

  class Person {
    var privateAge = 0

    var name: String = _

    def age = {
      _print("getter of age")
      privateAge
    }

    def age_=(newAge: Int) {
      _print("setter of age")
      privateAge = newAge
    }
  }

}
