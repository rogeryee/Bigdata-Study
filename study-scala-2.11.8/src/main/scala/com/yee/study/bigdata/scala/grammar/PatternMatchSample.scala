package com.yee.study.bigdata.scala.grammar

/**
 * 模式匹配示例
 *
 * 狭义的看，模式可以当作对某个类型，其内部数据在结构上抽象出来的表达式
 *
 * @author Roger.Yi
 */
object PatternMatchSample {

  def main(args: Array[String]): Unit = {
    // ------------ 示例 1
    // 匹配一个数组，它由三个元素组成，第一个元素为1，第二个元素为2，第三个元素为3
    Array(1, 2, 3) match {
      case Array(1, 2, 3) => println("ok")
    }

    // 匹配一个数组，它至少由一个元素组成，第一个元素为1
    Array(1, 2, 3) match {
      case Array(1, _*) => println("ok")
    }

    // 匹配一个List，它由三个元素组成，第一个元素为“A"，第二个元素任意类型，第三个元素为"C"
    List("A", "B", "C") match {
      case List("A", _, "C") => println("ok")
    }

    // ------------ 示例 2
    val a = 100

    // 常量模式，如果a与100相等则匹配成功
    a match {
      case 100 => println("ok")
    }

    // 类型模式，如果a是Int类型就匹配成功
    a match {
      case _: Int => println("ok")
    }

    // ------------ 示例 3 常量模式(constant patterns) 包含常量变量和常量字面量
    val site = "alibaba.com"
    site match {
      case "alibaba.com" => println("ok alibaba 1")
    }

    //注意这里常量必须以大写字母开头
    val ALIBABA = "alibaba.com"

    def foo(s: String) {
      s match {
        case ALIBABA => println("ok alibaba 2")
      }
    }

    foo("alibaba.com")

    // ------------ 示例 4 变量模式(variable patterns)
    site match {
      case whateverName => println(whateverName)
    }

    // ------------ 示例 5 通配符模式(wildcard patterns)
    // 通配符用下划线表示："_" ，可以理解成一个特殊的变量或占位符
    // case _ => 它可以匹配任何对象
    List(1, 2, 3) match {
      case List(_, _, 3) => println("示例 5 ok")
    }

    // ------------ 示例 6 构造器模式(constructor patterns)
    //抽象节点
    trait Node
    //具体的节点实现，有两个子节点
    case class TreeNode(v: String, left: Node, right: Node) extends Node
    //Tree，构造参数是根节点
    case class Tree(root: TreeNode)
    // 构造一个根节点含有2个子节点的数
    val tree = Tree(TreeNode("root", TreeNode("left", null, null), TreeNode("right", null, null)))

    tree.root match {
      case TreeNode(_, TreeNode("left", _, _), TreeNode("right", null, null)) =>
        println("示例 6 bingo")
    }

    // ------------ 示例 7 类型模式(type patterns)
    "hello" match {
      case _: String => println("示例 7 ok")
    }

    // ------------ 示例 8 变量绑定模式 (variable binding patterns)
    tree.root match {
      case TreeNode(_, leftNode@TreeNode("left", _, _), _) => leftNode
    }
  }
}
