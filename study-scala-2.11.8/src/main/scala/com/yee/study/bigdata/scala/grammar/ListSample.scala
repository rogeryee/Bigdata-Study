package com.yee.study.bigdata.scala.grammar

/**
 *
 * @author Roger.Yi
 */
object ListSample {

  def main(args: Array[String]): Unit = {
    val l1 = List(1, 2)
    val l2 = List(3, 4)

    // ::: 合并2个List
    val l4 = l1 ::: l2 // List(1, 2, 3, 4)

    // :: 是右操作符，下面两个语句等价
    val l5 = 1 :: 2 :: l2 // List(1, 2, 3, 4)
    val l6 = l2.::(2).::(1) // List(1, 2, 3, 4)

    // Nil 可以认为是一个空 List
    // 如果只写 val l7 = 1 :: 2 :: 3，由于 3 是 Int 类型，没有::方法，因此会导致编译失败。
    val l7 = 1 :: 2 :: 3 :: Nil // List(1, 2, 3)

    // List 的一些方法和作用
    // 创建带有三个值"Will"，"fill"和"until"的新 List[String]
    val thrill = "Will" :: "fill" :: "until" :: Nil

    // 返回在 thrill 列表上索引为 2(基于 0)的元素(返回"until")
    thrill(2)

    // 计算长度为4的String元素个数(返回2)
    thrill.count(s => s.length == 4)

    // 返回去掉前 2 个元素的 thrill 列表(返回 List("until"))
    thrill.drop(2)

    // 返回去掉后 2 个元素的 thrill 列表(返回 List("Will"))
    thrill.dropRight(2)

    // 判断是否有值为"until"的字串元素在thrill里(返回true)
    thrill.exists(s => s == "until")

    // 依次返回所有长度为 4 的元素组成的列表(返回 List("Will", "fill"))
    thrill.filter(s => s.length == 4)

    // 辨别是否thrill列表里所有元素都以"l"结尾(返回true)
    thrill.forall(s => s.endsWith("l"))

    // 对thrill列表每个字串执行print语句("Willfilluntil")
    thrill.foreach(s => print(s))
    thrill.foreach(print)

    // 返回 thrill 列表的第一个元素(返回"Will")
    thrill.head

    // 返回 thrill 列表除最后一个以外其他元素组成的列表(返回 List("Will", "fill"))
    thrill.init

    // 说明 thrill 列表是否为空(返回 false)
    thrill.isEmpty

    // 返回 thrill 列表的最后一个元素(返回"until")
    thrill.last

    // 返回 thrill 列表的元素数量(返回 3)
    thrill.length

    // 返回由thrill列表里每一个String元素都加了"y"构成的列表 (返回List("Willy", "filly", "untily"))
    thrill.map(s => s + "y")

    // 用列表的元素创建字串(返回"will, fill, until")
    thrill.mkString(", ")

    // 返回含有 thrill 列表的逆序元素的列表(返回 List("until", "fill", "Will"))
    thrill.reverse

    // 返回除掉第一个元素的 thrill 列表(返回 List("fill", "until"))
    thrill.tail
  }
}
