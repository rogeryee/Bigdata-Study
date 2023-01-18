package com.yee.study.bigdata.spark.scala.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Graphx Pregel 方法示例
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
object GraphxPregelSample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GraphSample")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 计算顶点5 到 各个顶点的最短距离
    //    shortestPath(sc)

    // 计算顶点1的二级粉丝
    followers2LevelOfV1(sc)
  }

  // 使用pregle算法计算 ，顶点5 到 各个顶点的最短距离
  def shortestPath(sc: SparkContext): Unit = {
    // 创建顶点
    val vertexRDD: RDD[(VertexId, (String, Int))] = sc.makeRDD(Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))))

    // 创建边，边的属性代表 相邻两个顶点之间的距离
    val edgeRDD: RDD[Edge[Int]] = sc.makeRDD(Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(2L, 5L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    ))

    // 创建图
    val graph1 = Graph(vertexRDD, edgeRDD)

    // (1) 設置頂點信息和每个顶点的值
    val srcVertexId = 5L
    val initialGraph: Graph[Double, Int] = graph1.mapVertices { case (vid, (name, age)) => if (vid == srcVertexId) 0.0 else Double.PositiveInfinity }

    println(" 1.每个顶点的属性值如下")
    initialGraph.vertices.collect().foreach(println)

    println("---------------开始调用pregel---------------")

    // (2) 調用pregel,返回的还是一个图
    val pregelGraph: Graph[Double, PartitionID] = initialGraph.pregel(
      Double.PositiveInfinity, //每个点的初始值，无穷大
      1, // Int.MaxValue, //最大迭代次数
      EdgeDirection.Out //发送消息的方向
    )(
      // vprog:接受到的消息和自己的消息进行合并
      // 这个顶点sendMsg发送的顶点信息
      // 三个参数 vprog: (VertexId, VD, A) => VD，
      // VertexId当前节点的顶点id，VD当前顶点的属性，A接收到的信息
      // 返回值：当前顶点更新后的属性
      (vid: VertexId, vd: Double, distMsg: Double) => {
        println(s"----------顶点${vid}调用vprog:接受到的消息和自己的消息进行合并----------------")
        // 即将接收到的信息和顶点属性进行比较，取最小值进行更新该顶点属性
        val minDist = math.min(vd, distMsg)
        println(s"顶点${vid}，顶点属性${vd}，收到消息${distMsg}，合并后的属性${minDist}")
        minDist
      },
      // sendMsg:发送消息,如果自己的消息+权重<目的地的消息，则发送
      // 一个参数 ： sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)]
      // 返回值：发送成功后的节点id和发送的消息的一个迭代器
      (edgeTriplet: EdgeTriplet[Double, PartitionID]) => {
        println(s"----------调用${edgeTriplet.srcId}调用sendMsg发送消息给顶点${edgeTriplet.dstId}------------")
        if (edgeTriplet.srcAttr + edgeTriplet.attr < edgeTriplet.dstAttr) {
          println(s"顶点${edgeTriplet.srcId}给顶点${edgeTriplet.dstId} 发送消息 ${edgeTriplet.srcAttr + edgeTriplet.attr}")
          Iterator[(VertexId, Double)]((edgeTriplet.dstId, edgeTriplet.srcAttr + edgeTriplet.attr))
        } else {
          Iterator.empty
        }
      },
      // mergeMsg多条接收消息，mergeMessage,取小合并多条消息
      // mergeMsg: (A, A) => A)
      (msg1: Double, msg2: Double) => {
        println(s"----------${msg1},${msg2}调用mergeMsg：合并多条接收消息------------")
        println(msg1, msg2)
        math.min(msg1, msg2)
      }
    )
  }

  /**
   * 计算节点1的二级粉丝 （边的dst节点表示粉丝）
   *
   * 思路：
   * 1. 将顶点1的属性设置成 2 （计算二级粉丝）
   * 2. 其余顶点的属性设置成 -1
   * 3. 从顶点1开始发送消息，每经过一条边则将边的src节点的属性设置成dst节点的属性-1
   * 4. 符合下列条件的边才能发送消息：
   * a) 边的 dst节点属性 > src节点属性
   * b) 边的 src节点属性 < 0
   *
   * @param sc
   */
  def followers2LevelOfV1(sc: SparkContext): Unit = {
    // 创建 VertexRDD（VertexId必须为Long）
    val usersRDD: RDD[(VertexId, (String, Long))] = sc.textFile("study-spark/data/users.txt")
      .map(line => line.split(","))
      .map(row => (row(0).toLong, (row(1), row(2).toLong)))

    // 创建 EdgeRDD（Edge.attr默认指定为1）
    val followersRDD: RDD[Edge[Long]] = sc.textFile("study-spark/data/followers2.txt")
      .map(line => line.split(" "))
      .map(row => Edge(row(0).toLong, row(1).toLong, 1))

    // ((2,(Gigi,25)),(1,(Andy,40)),1)
    // ((3,(Maggie,20)),(1,(Andy,40)),1)
    // ((4,(Annie,18)),(1,(Andy,40)),1)
    // ((5,(Amanda,24)),(2,(Gigi,25)),1)
    // ((6,(Matei,30)),(2,(Gigi,25)),1)
    // ((7,(Martin,35)),(3,(Maggie,20)),1)
    val graph: Graph[(String, Long), Long] = Graph(usersRDD, followersRDD)

    // 设置初始化图（顶点1设置成2，其余为-1）
    // ((2,-1),(1,2),1)
    // ((3,-1),(1,2),1)
    // ((4,-1),(1,2),1)
    // ((5,-1),(2,-1),1)
    // ((6,-1),(2,-1),1)
    // ((7,-1),(3,-1),1)
    val dstVid = 1L
    val inital_graph: Graph[Int, Long] = graph.mapVertices((vid, _) => if (vid == dstVid) 2 else -1)
    inital_graph.triplets.collect.foreach(println)

    val result_graph: Graph[Int, Long] = inital_graph.pregel(
      initialMsg = -1,
      maxIterations = 100,
      activeDirection = EdgeDirection.In
    )(
      vprog = (vid: VertexId, vd: Int, dstMsg: Int) => {
        // 若当前顶点已经可以到达0，则取0，否则取消息和当前属性的最大值
        val msg = if(dstMsg == 0) dstMsg else math.max(vd, dstMsg)
        println(s"vprog : Vertex[${vid}], dstMsg=${dstMsg} vd=${vd} msg=${msg}")
        msg
      },
      sendMsg = (edgeTriplet: EdgeTriplet[Int, Long]) => {
        if (
            (edgeTriplet.dstAttr > edgeTriplet.srcAttr && edgeTriplet.srcAttr < 0) ||
            (edgeTriplet.dstAttr >= edgeTriplet.srcAttr && edgeTriplet.srcAttr > 0)
        ) {
          println(s"sendMsg[OK]: srcId=${edgeTriplet.srcId} dstId=${edgeTriplet.dstId} srcAttr=${edgeTriplet.srcAttr} dstAttr=${edgeTriplet.dstAttr} msg=(${edgeTriplet.srcId}, ${edgeTriplet.dstAttr - 1})")
          Iterator[(VertexId, Int)]((edgeTriplet.srcId, edgeTriplet.dstAttr - 1))
        } else {
          println(s"sendMsg[Failed]: srcId=${edgeTriplet.srcId} dstId=${edgeTriplet.dstId} srcAttr=${edgeTriplet.srcAttr} dstAttr=${edgeTriplet.dstAttr}")
          Iterator.empty
        }
      },
      mergeMsg = (msg1: Int, msg2: Int) => {
        // 多个消息取最小值
        math.min(msg1, msg2)
      }
    )

    result_graph.subgraph(edgeTriplet => edgeTriplet.srcAttr == 0).triplets.collect.foreach(println)
//    result_graph.triplets.collect.foreach(println)
  }
}
