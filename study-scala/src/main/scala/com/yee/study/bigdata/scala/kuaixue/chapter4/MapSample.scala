package com.yee.study.bigdata.scala.kuaixue.chapter4

import com.yee.study.bigdata.scala.util.PrintUtil.{_print}

import scala.collection.mutable.{Map => MutableMap}

/**
 *
 * @author Roger.Yi
 */
object MapSample {

  def main(args: Array[String]): Unit = {
    // 不可变Map
    val scores = Map("Alice" -> 10, "Bob" -> 3, "Cindy" -> 8)

    // 可变
    val scoresMutable = MutableMap("Alice" -> 10, "Bob" -> 3, "Cindy" -> 8)
    val scoresMutable2 = MutableMap(("Alice", 10), ("Bob", 3), ("Cindy", 8))
    _print(scoresMutable)
    _print(scoresMutable2)

    val bobScore = scores("Bob") // 若无键值，则抛出异常
    val bobScore2 = if (scores.contains("Bob")) scores("Bob") else 0
    val bobScore3 = scores.getOrElse("Bob", 0)

    // 设置默认值
    val scoresWithDef = scoresMutable.withDefaultValue(0)
    val zeldaScore = scoresMutable.get("Zelda")
    val zeldaScoreWithDef = scoresWithDef.get("Zelda") // 0, Zelda 不存在
    println(zeldaScore)
    println(zeldaScoreWithDef)

    // 更新
    scoresMutable("Bob") = 10
    scoresMutable("Fred") = 7
    _print("step1", scoresMutable)

    scoresMutable += ("Bob" -> 3, "Fred" -> 2)
    _print("step2", scoresMutable)

    scoresMutable -= "Alice" // 移除键值
    _print("step3", scoresMutable)

    val newScores = scores + ("Bob" -> 10, "Roger" -> 1)
    _print(newScores)
  }
}
