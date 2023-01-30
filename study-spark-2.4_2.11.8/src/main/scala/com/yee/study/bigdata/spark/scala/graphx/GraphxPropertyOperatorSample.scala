package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Graph 属性方法 示例
 *
 * mapVertices 遍历所有顶点
 * mapEdges 遍历所有边
 * mapTriplets 遍历所有三元组
 *
 * reverse 将每条边的方向反向
 * subgraph 过滤一些符合条件的边和顶点构造子图
 * mask 返回和另外一个graph的公共子图
 * groupEdges 可以对平行边进行merge，但要求平行边位于相同的分区。
 *
 * @author Roger.Yi
 */
object GraphxPropertyOperatorSample {

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

    // mapVertices 遍历所有顶点：每个人的年龄*2
    // ((2,(Gigi,50)),(1,(Andy,80)),1)
    // ((3,(Maggie,40)),(1,(Andy,80)),1)
    // ((4,(Annie,36)),(1,(Andy,80)),1)
    // ((5,(Amanda,48)),(2,(Gigi,50)),1)
    // ((6,(Matei,60)),(2,(Gigi,50)),1)
    // ((7,(Martin,70)),(3,(Maggie,40)),1)
    val subGraph1: Graph[(String, Long), Long] = graph.mapVertices((vid: VertexId, attr: (String, Long)) => (attr._1, attr._2 * 2))
//    subGraph1.triplets.collect.foreach(println)

    // mapEdges 遍历所有边：变更边的属性为 src->dst
    // ((2,(Gigi,25)),(1,(Andy,40)),2->1)
    // ((3,(Maggie,20)),(1,(Andy,40)),3->1)
    // ((4,(Annie,18)),(1,(Andy,40)),4->1)
    // ((5,(Amanda,24)),(2,(Gigi,25)),5->2)
    // ((6,(Matei,30)),(2,(Gigi,25)),6->2)
    // ((7,(Martin,35)),(3,(Maggie,20)),7->3)
    val subGraph2: Graph[(String, Long), String] = graph.mapEdges(edge => edge.srcId + "->" + edge.dstId)
//    subGraph2.triplets.collect.foreach(println)

    // mapTriplets 遍历所有三元组
    // ((2,(Gigi,25)),(1,(Andy,40)),(1,10))
    // ((3,(Maggie,20)),(1,(Andy,40)),(1,10))
    // ((4,(Annie,18)),(1,(Andy,40)),(1,10))
    // ((5,(Amanda,24)),(2,(Gigi,25)),(1,10))
    // ((6,(Matei,30)),(2,(Gigi,25)),(1,10))
    // ((7,(Martin,35)),(3,(Maggie,20)),(1,10))
    val subGraph3: Graph[(String, Long), (Long, Int)] = graph.mapTriplets(triplet => (triplet.attr, 10))
//    subGraph3.triplets.collect.foreach(println)

    // reverse 将每条边的方向反向
    // ((1,(Andy,40)),(2,(Gigi,25)),1)
    // ((1,(Andy,40)),(3,(Maggie,20)),1)
    // ((1,(Andy,40)),(4,(Annie,18)),1)
    // ((2,(Gigi,25)),(5,(Amanda,24)),1)
    // ((2,(Gigi,25)),(6,(Matei,30)),1)
    // ((3,(Maggie,20)),(7,(Martin,35)),1)
    val subGraph4: Graph[(String, Long), Long] = graph.reverse
//    subGraph4.triplets.collect.foreach(println)

    // subgraph 过滤一些符合条件的边和顶点构造子图：过滤与顶点1相连的边
    // ((5,(Amanda,24)),(2,(Gigi,25)),1)
    // ((6,(Matei,30)),(2,(Gigi,25)),1)
    // ((7,(Martin,35)),(3,(Maggie,20)),1)
    val subGraph5: Graph[(String, Long), Long] = graph.subgraph(edgeTriplet => (edgeTriplet.srcId != 1 && edgeTriplet.dstId != 1))
//    subGraph5.triplets.collect.foreach(println)

    // 过滤与顶点2相连的边
    // ((3,(Maggie,20)),(1,(Andy,40)),1)
    // ((4,(Annie,18)),(1,(Andy,40)),1)
    // ((7,(Martin,35)),(3,(Maggie,20)),1)
    val subGraph6: Graph[(String, Long), Long] = graph.subgraph(edgeTriplet => (edgeTriplet.srcId != 2 && edgeTriplet.dstId != 2))
//    subGraph6.triplets.collect.foreach(println)

    // mask 返回 subGraph5 和 subGraph6 的公共子图
    // ((7,(Martin,35)),(3,(Maggie,20)),1)
    val subGraph7: Graph[(String, Long), Long] = subGraph5.mask(subGraph6)
//    subGraph7.triplets.collect.foreach(println)
  }
}
