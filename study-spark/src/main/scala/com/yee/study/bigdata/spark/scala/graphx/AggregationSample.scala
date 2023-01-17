package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, GraphLoader, VertexRDD}
import org.apache.spark.sql.SparkSession

/**
 * 使用GraphX计算粉丝平均年龄
 *
 * @author Roger.Yi
 */
object AggregationSample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GraphSample")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val users = sc.textFile("study-spark/data/users.txt")
      .map(line => line.split(","))
      .map(row => (row(0).toLong, (row(1), row(2).toLong)))
    val followerGraph = GraphLoader.edgeListFile(sc, "study-spark/data/followers.txt")

    // 添加用户属性
    val graph = followerGraph.outerJoinVertices(users) {
      (_, _, attr) => attr.get
    }

    graph.triplets.collect.foreach(println)
  }
}
