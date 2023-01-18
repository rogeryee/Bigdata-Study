package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Graph 收集邻居信息 示例
 *
 * collectNeighborIds 收集邻居ID
 * collectNeighbors 收集邻居
 *
 * @author Roger.Yi
 */
object GraphxNeighborSample {

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
    val users: RDD[(VertexId, (String, Long))] = sc.textFile("study-spark/data/users.txt")
      .map(line => line.split(","))
      .map(row => (row(0).toLong, (row(1), row(2).toLong)))

    // 创建 EdgeRDD（Edge.attr默认指定为1）
    val followers: RDD[Edge[Long]] = sc.textFile("study-spark/data/followers.txt")
      .map(line => line.split(" "))
      .map(row => Edge(row(0).toLong, row(1).toLong, 1))

    val graph: Graph[(String, Long), Long] = Graph(users, followers)
    //    graph.triplets.collect.foreach(println)

    // 采集每个节点的 In 的邻居节点
    // VertexId[4]=
    // VertexId[6]=
    // VertexId[2]=(5,(Amanda,24))(6,(Matei,30))
    // VertexId[1]=(2,(Gigi,25))(3,(Maggie,20))(4,(Annie,18))
    // VertexId[3]=(7,(Martin,35))
    // VertexId[7]=
    // VertexId[5]=
    graph.collectNeighbors(EdgeDirection.In).collect.foreach(x => {
      print("VertexId[" + x._1 + "]=")
      x._2.foreach(print)
      println()
    })
  }
}
