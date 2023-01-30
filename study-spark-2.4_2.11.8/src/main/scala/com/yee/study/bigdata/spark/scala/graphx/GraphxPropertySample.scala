package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Graph 属性 示例
 *
 * @author Roger.Yi
 */
object GraphxPropertySample {

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

    // Degrees = inDegrees + outDegrees
    // (4,1)
    // (6,1)
    // (2,3)
    // (1,3)
    // (3,2)
    // (7,1)
    // (5,1)
    graph.degrees.collect().foreach(println)

    // inDegrees 入度
    // (2,2)
    // (1,3)
    // (3,1)
    graph.inDegrees.collect().foreach(println)

    // outDegrees 出度
    // (4,1)
    // (6,1)
    // (2,1)
    // (3,1)
    // (7,1)
    // (5,1)
    graph.outDegrees.collect().foreach(println)

    // Vertice视图（顶点）
    // (4,(Annie,18))
    // (6,(Matei,30))
    // (2,(Gigi,25))
    // (1,(Andy,40))
    // (3,(Maggie,20))
    // (7,(Martin,35))
    // (5,(Amanda,24))
    graph.vertices.collect.foreach(println)

    // Edge视图（边）
    // Edge(2,1,1)
    // Edge(3,1,1)
    // Edge(4,1,1)
    // Edge(5,2,1)
    // Edge(6,2,1)
    // Edge(7,3,1)
    graph.edges.collect.foreach(println)

    // Triplet视图（包含了边以及与之关联的顶点属性）
    // ((2,(Gigi,25)),(1,(Andy,40)),1)
    // ((3,(Maggie,20)),(1,(Andy,40)),1)
    // ((4,(Annie,18)),(1,(Andy,40)),1)
    // ((5,(Amanda,24)),(2,(Gigi,25)),1)
    // ((6,(Matei,30)),(2,(Gigi,25)),1)
    // ((7,(Martin,35)),(3,(Maggie,20)),1)
    graph.triplets.collect.foreach(println)
  }
}
