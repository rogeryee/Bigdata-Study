package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Graphx Pregel 方法更多示例
 *
 * pregel迭代接口有2个参数列表：
 *
 * 第一个参数列表完成了一些配置工作，三个参数分别是 initialMsg、maxIter 和 activeDirection。
 * 分别设置了初始消息，最大迭代次数和边激活的条件。
 *
 * 第二个参数列表有三个函数参数：vprog、sendMsg和mergeMsg.
 *
 * 1. vprog是顶点更新函数，它在每轮迭代的最后一步用mergeMsg的结果更新顶点属性，并在初始化时用initialMsg初始化图。
 *
 * 2. sengMsg是消息发送函数。其输入参数类型为EdgeTriplet，输出参数类型为一个Iterator，与aggregateMessages中的sengMsg有所不同。
 * 需要注意的是，为了让算法结束迭代，需要在合适的时候让其返回一个空的Iterator
 *
 * 3. mergeMsg是消息合并函数。与aggregateMessages中的mergeMsg一样。
 *
 * @author Roger.Yi
 */
object GraphxPregelMoreSample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GraphSample")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 最短路径算法
    dijkstra(sc)
  }

  /**
   * 找到图中各个顶点到给定顶点（A)的最短路径。
   *
   * 最短路径算法(Dijkstra)
   *
   * @param sc
   */
  def dijkstra(sc: SparkContext): Unit = {
    val vertexRDD: RDD[(VertexId, String)] = sc.makeRDD(Array(
      (1L, "A"),
      (2L, "B"),
      (3L, "C"),
      (4L, "D"),
      (5L, "E"),
      (6L, "F"),
      (7L, "G")
    ))

    val edgeRDD: RDD[Edge[Double]] = sc.makeRDD(
      Array(
        Edge(1L, 2L, 7.0),
        Edge(1L, 4L, 5.0),
        Edge(2L, 3L, 8.0),
        Edge(2L, 4L, 9.0),
        Edge(2L, 5L, 7.0),
        Edge(3L, 5L, 5.0),
        Edge(5L, 6L, 8.0),
        Edge(4L, 5L, 15.0),
        Edge(4L, 6L, 6.0),
        Edge(5L, 7L, 9.0),
        Edge(6L, 7L, 11.0)
      ))

    // ((1,A),(2,B),7.0)
    // ((1,A),(4,D),5.0)
    // ((2,B),(3,C),8.0)
    // ((2,B),(4,D),9.0)
    // ((2,B),(5,E),7.0)
    // ((3,C),(5,E),5.0)
    // ((5,E),(6,F),8.0)
    // ((4,D),(5,E),15.0)
    // ((4,D),(6,F),6.0)
    // ((5,E),(7,G),9.0)
    // ((6,F),(7,G),11.0)
    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD)
    graph.triplets.collect().foreach(println)

    val init_graph = graph.mapVertices((vid, vd) => if (vid == 1) 0.0 else Double.PositiveInfinity)
    val result_graph = init_graph.pregel(
      initialMsg = Double.PositiveInfinity,
      activeDirection = EdgeDirection.Out
    )(
      vprog = (vid, vd, msg) => {
        math.min(vd, msg)
      },
      sendMsg = (edgeTriplet) => {
        val candidate = edgeTriplet.srcAttr + edgeTriplet.attr
        if (candidate >= edgeTriplet.dstAttr) {
          // 源顶点 + 边 大于等于 目标点值，则无需再计算
          Iterator.empty
        } else {
          Iterator[(Long, Double)]((edgeTriplet.dstId, candidate))
        }
      },
      mergeMsg = (a, b) => {
        math.min(a, b)
      }
    )

    // 合并顶点名称
    val output = result_graph.outerJoinVertices[String, (String, Double)](vertexRDD)((vid, vd, u) => (u.getOrElse("Unknown"), vd))

    // (A,0.0)
    // (B,7.0)
    // (C,15.0)
    // (D,5.0)
    // (E,14.0)
    // (F,11.0)
    // (G,22.0)
    output.vertices.map(v => v._2).collect.foreach(println)
  }
}
