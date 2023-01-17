package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Aggregate Messages 示例
 *
 * @author Roger.Yi
 */
object GraphSample2 {

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

    // Create a graph with "age" as the vertex property.
    val graph: Graph[Double, Int] =
      GraphGenerators.logNormalGraph(sc, numVertices = 3).mapVertices((id, _) => id.toDouble)

//    graph.vertices.collect.foreach(println)
//    graph.edges.collect.foreach(println)
    graph.triplets.map(triplet => "(" + triplet.srcAttr + ") is the " + triplet.attr + " of (" + triplet.dstAttr + ")").collect.foreach(println)

    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] =
      graph.aggregateMessages[(Int, Double)](
        triplet => { // Map Function
          if (triplet.srcAttr > triplet.dstAttr) {
            // Send message to destination vertex containing counter and age
            triplet.sendToDst((1, triplet.srcAttr))
          }
        },
        // Add counter and age
//        (a, b) => (a._1 + b._1, a._2 + b._2)
        (a, b) => if(a._2 > b._2) a else b
      )

    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues((id, value) => value match { case (count, totalAge) => totalAge / count})

    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println)
  }
}
