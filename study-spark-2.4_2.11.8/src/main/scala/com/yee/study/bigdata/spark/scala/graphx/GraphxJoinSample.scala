package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Graph 示例
 *
 * joinVertices 不能修改属性
 * outerJoinVertices 可以修改属性
 *
 * @author Roger.Yi
 */
object GraphxJoinSample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GraphSample")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // joinVertices 示例
    joinVertices(sc)

    // outerJoinVertices 示例
    outerJoinVertices(sc)
  }

  def joinVertices(sc: SparkContext): Unit = {
    val distance_graph: Graph[String, Double] = Graph.fromEdges(sc.makeRDD(Array(
      Edge(1L, 2L, 10.0),
      Edge(1L, 2L, 3.0),
      Edge(2L, 3L, 5.0),
      Edge(2L, 3L, 7.0),
      Edge(1L, 4L, 2.0)
    )), "1")

    // ((1,1),(2,1),10.0)
    // ((1,1),(2,1),3.0)
    // ((2,1),(3,1),5.0)
    // ((2,1),(3,1),7.0)
    // ((1,1),(4,1),2.0)
    distance_graph.triplets.collect.foreach(println)

    val rdd_city: RDD[(VertexId, String)] = sc.makeRDD(Array(
      (1L, "Beijing"),
      (2L, "Nanjing"),
      (3L, "Shanghai"),
      (4L, "Tianjing")
    ))

    // 注意这里的 v 和 u 的类型需要一致
    val join_graph = distance_graph.joinVertices(rdd_city)((vid, v, u) => u)
    join_graph.triplets.collect.foreach(println)
  }

  def outerJoinVertices(sc: SparkContext): Unit = {
    val myVerticesRDD: RDD[(VertexId, String)] = sc.makeRDD(Array(
      (1L, "Ann"),
      (2L, "Bill"),
      (3L, "Charles"),
      (4L, "Diane"),
      (5L, "Went to gym this morning")
    ))

    val myEdgesRDD: RDD[Edge[String]] = sc.makeRDD(Array(
      Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"),
      Edge(3L, 5L, "wrote-status"),
      Edge(4L, 5L, "like-status")
    ))

    // ((1,Ann),(2,Bill),is-friends-with)
    // ((2,Bill),(3,Charles),is-friends-with)
    // ((3,Charles),(4,Diane),is-friends-with)
    // ((3,Charles),(5,Went to gym this morning),wrote-status)
    // ((4,Diane),(5,Went to gym this morning),like-status)
    val myGraph: Graph[String, String] = Graph(myVerticesRDD, myEdgesRDD)
    myGraph.triplets.collect.foreach(println)

    // 性别RDD
    val rdd_gender = sc.makeRDD(Array(
      (1L, "female"),
      (2L, "male"),
      (3L, "male"),
      (4L, "female")
    ))

    // outerJoinVertices 为每个顶点增加性别属性
    // ((1,(Ann,female)),(2,(Bill,male)),is-friends-with)
    // ((2,(Bill,male)),(3,(Charles,male)),is-friends-with)
    // ((3,(Charles,male)),(4,(Diane,female)),is-friends-with)
    // ((3,(Charles,male)),(5,(Went to gym this morning,Unknown)),wrote-status)
    // ((4,(Diane,female)),(5,(Went to gym this morning,Unknown)),like-status)
    val graph_outerjoin: Graph[(String, String), String] = myGraph.outerJoinVertices(rdd_gender)((vid, v, opt) => (v, opt.getOrElse("Unknown")))
    graph_outerjoin.triplets.collect.foreach(println)
  }
}
