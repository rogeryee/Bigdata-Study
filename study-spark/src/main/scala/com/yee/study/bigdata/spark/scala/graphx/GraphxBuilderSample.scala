package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Graph 构造 示例
 *
 * @author Roger.Yi
 */
object GraphxBuilderSample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GraphSample")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 基于VertexRDD、EdgeRDD 构建Graph对象
    // ((2,(Gigi,25)),(1,(Andy,40)),1)
    // ((3,(Maggie,20)),(1,(Andy,40)),1)
    // ((4,(Annie,18)),(1,(Andy,40)),1)
    // ((5,(Amanda,24)),(2,(Gigi,25)),1)
    // ((6,(Matei,30)),(2,(Gigi,25)),1)
    // ((7,(Martin,35)),(3,(Maggie,20)),1)
    val graph1: Graph[(String, Long), Long] = createGraphByConstructorByRDD(sc)
    //    graph1.triplets.collect.foreach(println)

    // 基于EdgeRDD构建Graph对象，节点属性为默认值 defaultValue
    // ((2,defaultValue),(1,defaultValue),1)
    // ((3,defaultValue),(1,defaultValue),1)
    // ((4,defaultValue),(1,defaultValue),1)
    // ((5,defaultValue),(2,defaultValue),1)
    // ((6,defaultValue),(2,defaultValue),1)
    // ((7,defaultValue),(3,defaultValue),1)
    val graph2: Graph[Any, Long] = createGraphByConstructorByEdge(sc)
    //    graph2.triplets.collect.foreach(println)

    // 基于EdgeTuple构建Graph对象，节点属性为默认值 defaultValue
    // ((2,defaultValue),(1,defaultValue),1)
    // ((3,defaultValue),(1,defaultValue),1)
    // ((4,defaultValue),(1,defaultValue),1)
    // ((5,defaultValue),(2,defaultValue),1)
    // ((6,defaultValue),(2,defaultValue),1)
    // ((7,defaultValue),(3,defaultValue),1)
    val graph3: Graph[String, Int] = createGraphByConstructorByEdgeTuple(sc)
    //    graph3.triplets.collect.foreach(println)

    // 基于 Edge文件构建Graph对象
    // ((2,1),(1,1),1)
    // ((3,1),(1,1),1)
    // ((4,1),(1,1),1)
    // ((5,1),(2,1),1)
    // ((6,1),(2,1),1)
    // ((7,1),(3,1),1)
    val graph4: Graph[Int, Int] = createGraphByEdgeFile(sc)
    graph4.triplets.collect.foreach(println)
  }

  /**
   * 通过Graph的构造器创建 Graph 对象（基于VertexRDD、EdgeRDD）
   *
   * @param sc
   * @return
   */
  def createGraphByConstructorByRDD(sc: SparkContext): Graph[(String, Long), Long] = {
    // 创建 VertexRDD（VertexId必须为Long）
    val users: RDD[(VertexId, (String, Long))] = sc.textFile("study-spark/data/users.txt")
      .map(line => line.split(","))
      .map(row => (row(0).toLong, (row(1), row(2).toLong)))

    // 创建 EdgeRDD（Edge.attr默认指定为1）
    val followers: RDD[Edge[Long]] = sc.textFile("study-spark/data/followers.txt")
      .map(line => line.split(" "))
      .map(row => Edge(row(0).toLong, row(1).toLong, 1))

    val graph: Graph[(String, Long), Long] = Graph(users, followers)
    graph
  }

  /**
   * 通过Graph的构造器创建 Graph 对象（基于 EdgeRDD）
   *
   * @param sc
   * @return
   */
  def createGraphByConstructorByEdge(sc: SparkContext): Graph[Any, Long] = {
    // 创建 EdgeRDD（Edge.attr默认指定为1）
    val followers: RDD[Edge[Long]] = sc.textFile("study-spark/data/followers.txt")
      .map(line => line.split(" "))
      .map(row => Edge(row(0).toLong, row(1).toLong, 1))

    val graph: Graph[Any, Long] = Graph.fromEdges(followers, "defaultValue")
    graph
  }

  /**
   * 通过Graph的构造器创建 Graph 对象（基于 EdgeTuple）
   *
   * @param sc
   * @return
   */
  def createGraphByConstructorByEdgeTuple(sc: SparkContext): Graph[String, Int] = {
    // 创建 EdgeRDD（Edge.attr默认指定为1）
    val followers: RDD[(Long, Long)] = sc.textFile("study-spark/data/followers.txt")
      .map(line => line.split(" "))
      .map(row => (row(0).toLong, row(1).toLong))

    val graph: Graph[String, Int] = Graph.fromEdgeTuples(followers, "defaultValue")
    graph
  }

  /**
   * 通过Graph的构造器创建 Graph 对象（基于 Edge文件）
   *
   * @param sc
   * @return
   */
  def createGraphByEdgeFile(sc: SparkContext): Graph[Int, Int] = {
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "study-spark/data/followers.txt")
    graph
  }
}
