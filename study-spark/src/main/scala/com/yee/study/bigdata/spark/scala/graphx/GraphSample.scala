package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 *
 * @author Roger.Yi
 */
object GraphSample {

  /**
   * 设置master的两种方式：
   * 1. 设置运行变量
   * a) Run > Edit Configurations... > Application > "My project name" > Configuraton
   * b) VM options: -Dspark.master=local
   *
   * 2. 配置master属性
   * val spark = SparkSession.builder().appName("xxx").master("local[*]").getOrCreate()
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GraphSample")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))

    // Create an RDD fro edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),
        Edge(5L, 0L, "colleague")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build and initial Graph
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    // Count all user which are postdoc
    println(graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count)

    // Count all the edges where src < dst
    println(graph.edges.filter(e => e.srcId < e.dstId).count)

    // Use the triplets view to create an RDD od facts
    val facts: RDD[String] = graph.triplets.map(triplet => "(" + triplet.srcAttr._1 + "," + triplet.srcAttr._2 + ") is the " + triplet.attr + " of (" + triplet.dstAttr._1 + "," + triplet.dstAttr._2 + ")")
    facts.collect.foreach(println)

    // Use the implicit GrpahOps.inDegree operator
    val inDegree: VertexRDD[Int] = graph.inDegrees

    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")

    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println)
    validGraph.triplets.map(triplet => "(" + triplet.srcAttr._1 + "," + triplet.srcAttr._2 + ") is the " + triplet.attr + " of (" + triplet.dstAttr._1 + "," + triplet.dstAttr._2 + ")").collect.foreach(println)
  }
}
