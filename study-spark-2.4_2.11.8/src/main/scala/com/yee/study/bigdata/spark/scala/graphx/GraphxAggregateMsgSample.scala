package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 使用GraphX计算粉丝平均年龄和最大年龄 (基于 aggregateMessage 方法)
 *
 * aggregateMessages 在图结构中实现了一个基本的map/reduce编程模型。
 * sendMsg 是map过程，每条边向其src或dst发送一个消息。其输入参数为EdgeContext类型。EdgeContext类型比Triplet类型多了sendToSrc和sendToDst两个方法，用于发送消息。
 * mergeMsg 是reduce过程，每个顶点收集其收到的消息，并做合并处理。
 * aggregateMessages 的返回值是一个VetexRDD
 *
 * @author Roger.Yi
 */
object GraphxAggregateMsgSample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GraphSample")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // ((2,(Gigi,25)),(1,(Andy,40)),1)
    // ((3,(Maggie,20)),(1,(Andy,40)),1)
    // ((4,(Annie,18)),(1,(Andy,40)),1)
    // ((5,(Amanda,24)),(2,(Gigi,25)),1)
    // ((6,(Matei,30)),(2,(Gigi,25)),1)
    // ((7,(Martin,35)),(3,(Maggie,20)),1)
    // 创建 VertexRDD（VertexId必须为Long）
    val usersRDD: RDD[(VertexId, (String, Long))] = sc.textFile("study-spark/data/users.txt")
      .map(line => line.split(","))
      .map(row => (row(0).toLong, (row(1), row(2).toLong)))

    // 创建 EdgeRDD（Edge.attr默认指定为1）
    val followersRDD: RDD[Edge[Long]] = sc.textFile("study-spark/data/followers.txt")
      .map(line => line.split(" "))
      .map(row => Edge(row(0).toLong, row(1).toLong, 1))

    val graph: Graph[(String, Long), Long] = Graph(usersRDD, followersRDD)
    graph.triplets.collect.foreach(println)

    // 计算粉丝平均年龄
    avgAgeOfFollowers(graph)

    // 计算粉丝最大年龄
    maxAgeOfFollowers(graph)
  }

  // 计算粉丝平均年龄
  def avgAgeOfFollowers(graph: Graph[(String, Long), Long]): Unit = {
    // 计算粉丝数量以及年龄和
    // def aggregateMessages[A: ClassTag](
    //      sendMsg: EdgeContext[VD, ED, A] => Unit,
    //      mergeMsg: (A, A) => A,
    //      tripletFields: TripletFields = TripletFields.All)
    val followers: VertexRDD[(Int, Long)] = graph.aggregateMessages[(Int, Long)](
      triplet => {
        // 发送顶点数和年龄到被关注者
        triplet.sendToDst((1, triplet.srcAttr._2))
      },

      // 累计粉丝数量和年龄
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )

    // 计算平均年龄
    val avgAgeOfFollowers: VertexRDD[Double] =
      followers.mapValues((id, value) => value match {
        case (count, totalAge) => totalAge / count
      })

    // (2,27.0)
    // (1,21.0)
    // (3,35.0)
    avgAgeOfFollowers.collect.foreach(println)
  }

  // 计算粉丝最大年龄
  def maxAgeOfFollowers(graph: Graph[(String, Long), Long]): Unit = {
    // 计算粉丝最大年龄
    // def aggregateMessages[A: ClassTag](
    //      sendMsg: EdgeContext[VD, ED, A] => Unit,
    //      mergeMsg: (A, A) => A,
    //      tripletFields: TripletFields = TripletFields.All)
    val followers: VertexRDD[Long] = graph.aggregateMessages[Long](
      triplet => {
        // 发送年龄到被关注者
        triplet.sendToDst(triplet.srcAttr._2)
      },

      // 比较年龄，取较大值
      (a, b) => if (a > b) a else b
    )

    // 计算最大年龄
    val maxAgeOfFollowers: VertexRDD[Long] =
      followers.mapValues(x => x)

    // (2,30)
    // (1,25)
    // (3,35)
    maxAgeOfFollowers.collect.foreach(println)
  }
}
