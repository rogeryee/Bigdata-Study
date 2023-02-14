package com.yee.study.bigdata.flink114.scala.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

/**
 * Flink SQL - Query 示例
 *
 * @author Roger.Yi
 */
object SqlQuerySample {

  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(sEnv)

    // Window TVF 示例
    //    windowTVFSamples(sEnv, tEnv)

    // Window Join 示例
//    windowJoinSample(sEnv, tEnv)
  }

  /**
   * Window TVF, Aggregation 示例
   *
   * @param sEnv
   * @param tabEnv
   */
  def windowTVFSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val dataStream = sEnv.fromElements(
      Bid("C", 4.00F, "supplier1", LocalDateTime.parse("2022-04-15 08:05:00", formatter).atZone(ZoneId.systemDefault()).toInstant),
      Bid("A", 2.00F, "supplier1", LocalDateTime.parse("2022-04-15 08:07:00", formatter).atZone(ZoneId.systemDefault()).toInstant),
      Bid("D", 5.00F, "supplier2", LocalDateTime.parse("2022-04-15 08:09:00", formatter).atZone(ZoneId.systemDefault()).toInstant),
      Bid("B", 3.00F, "supplier2", LocalDateTime.parse("2022-04-15 08:11:00", formatter).atZone(ZoneId.systemDefault()).toInstant),
      Bid("E", 1.00F, "supplier1", LocalDateTime.parse("2022-04-15 08:13:00", formatter).atZone(ZoneId.systemDefault()).toInstant),
      Bid("F", 6.00F, "supplier2", LocalDateTime.parse("2022-04-15 08:17:00", formatter).atZone(ZoneId.systemDefault()).toInstant)
    )

    tabEnv.createTemporaryView(
      "Bid",
      dataStream,
      Schema.newBuilder()
        .columnByExpression("rowtime", "CAST(bidtime AS TIMESTAMP_LTZ(3))")
        .watermark("rowtime", "rowtime - INTERVAL '1' SECOND")
        .build()
    )

    /**
     * TUMBLE:
     * TUMBLE(TABLE data, DESCRIPTOR(timecol), size [, offset ])
     *
     * 计算每10分钟的窗口内的物品的总价
     * +----+-------------------------+-------------------------+--------------------------------+
     * | op |            window_start |              window_end |                         EXPR$2 |
     * +----+-------------------------+-------------------------+--------------------------------+
     * | +I | 2022-04-15 08:00:00.000 | 2022-04-15 08:10:00.000 |                           11.0 |
     * | +I | 2022-04-15 08:10:00.000 | 2022-04-15 08:20:00.000 |                           10.0 |
     * +----+-------------------------+-------------------------+--------------------------------+
     */
    // 写法1
    tabEnv.executeSql(
      """
        |SELECT
        |  window_start
        |, window_end
        |, SUM(price)
        |FROM TABLE(
        | TUMBLE(TABLE Bid, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES)
        |)
        |GROUP BY window_start, window_end
        |""".stripMargin)
      .print()

    // 写法2
    tabEnv.executeSql(
      """
        |SELECT
        |  TUMBLE_START(rowtime, INTERVAL '10' MINUTES) as w_start
        |, TUMBLE_END(rowtime, INTERVAL '10' MINUTES) as w_end
        |, SUM(price)
        |FROM Bid
        |GROUP BY TUMBLE(rowtime, INTERVAL '10' MINUTES)
        |""".stripMargin)
      .print()

    /**
     * HOP:
     * HOP(TABLE data, DESCRIPTOR(timecol), slide, size [, offset ])
     *
     * 每5钟计算10分钟内的物品总价
     *
     * +----+-------------------------+-------------------------+--------------------------------+
     * | op |            window_start |              window_end |                         EXPR$2 |
     * +----+-------------------------+-------------------------+--------------------------------+
     * | +I | 2022-04-15 08:00:00.000 | 2022-04-15 08:10:00.000 |                           11.0 |
     * | +I | 2022-04-15 08:05:00.000 | 2022-04-15 08:15:00.000 |                           15.0 |
     * | +I | 2022-04-15 08:10:00.000 | 2022-04-15 08:20:00.000 |                           10.0 |
     * | +I | 2022-04-15 08:15:00.000 | 2022-04-15 08:25:00.000 |                            6.0 |
     * +----+-------------------------+-------------------------+--------------------------------+
     */
    // 写法1：
    tabEnv.executeSql(
      """
        |SELECT
        |  window_start
        |, window_end
        |, SUM(price)
        |FROM TABLE(
        | HOP(TABLE Bid, DESCRIPTOR(rowtime), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES)
        |)
        |GROUP BY window_start, window_end
        |""".stripMargin)
      .print()

    // 写法2
    tabEnv.executeSql(
      """
        |SELECT
        |  HOP_START(rowtime, INTERVAL '5' MINUTES, INTERVAL '10' MINUTES) as w_start
        |, HOP_END(rowtime, INTERVAL '5' MINUTES, INTERVAL '10' MINUTES) as w_end
        |, SUM(price)
        |FROM Bid
        |GROUP BY HOP(rowtime, INTERVAL '5' MINUTES, INTERVAL '10' MINUTES)
        |""".stripMargin)
      .print()

    /**
     * CUMULATE:
     * CUMULATE(TABLE data, DESCRIPTOR(timecol), step, size)
     *
     * 计算每2分钟的窗口的累计值（窗口大小为10分钟）
     *
     * +----+-------------------------+-------------------------+--------------------------------+
     * | op |                 w_start |                   w_end |                         EXPR$2 |
     * +----+-------------------------+-------------------------+--------------------------------+
     * | +I | 2022-04-15 07:56:00.000 | 2022-04-15 08:06:00.000 |                            4.0 |
     * | +I | 2022-04-15 07:58:00.000 | 2022-04-15 08:08:00.000 |                            6.0 |
     * | +I | 2022-04-15 08:00:00.000 | 2022-04-15 08:10:00.000 |                           11.0 |
     * | +I | 2022-04-15 08:02:00.000 | 2022-04-15 08:12:00.000 |                           14.0 |
     * | +I | 2022-04-15 08:04:00.000 | 2022-04-15 08:14:00.000 |                           15.0 |
     * | +I | 2022-04-15 08:06:00.000 | 2022-04-15 08:16:00.000 |                           11.0 |
     * | +I | 2022-04-15 08:08:00.000 | 2022-04-15 08:18:00.000 |                           15.0 |
     * | +I | 2022-04-15 08:10:00.000 | 2022-04-15 08:20:00.000 |                           10.0 |
     * | +I | 2022-04-15 08:12:00.000 | 2022-04-15 08:22:00.000 |                            7.0 |
     * | +I | 2022-04-15 08:14:00.000 | 2022-04-15 08:24:00.000 |                            6.0 |
     * | +I | 2022-04-15 08:16:00.000 | 2022-04-15 08:26:00.000 |                            6.0 |
     * +----+-------------------------+-------------------------+--------------------------------+
     */
    // 写法1：
    tabEnv.executeSql(
      """
        |SELECT
        |  window_start
        |, window_end
        |, SUM(price)
        |FROM TABLE(
        | HOP(TABLE Bid, DESCRIPTOR(rowtime), INTERVAL '2' MINUTES, INTERVAL '10' MINUTES)
        |)
        |GROUP BY window_start, window_end
        |""".stripMargin)
      .print()

    // 写法2
    tabEnv.executeSql(
      """
        |SELECT
        |  HOP_START(rowtime, INTERVAL '2' MINUTES, INTERVAL '10' MINUTES) as w_start
        |, HOP_END(rowtime, INTERVAL '2' MINUTES, INTERVAL '10' MINUTES) as w_end
        |, SUM(price)
        |FROM Bid
        |GROUP BY HOP(rowtime, INTERVAL '2' MINUTES, INTERVAL '10' MINUTES)
        |""".stripMargin)
      .print()

    /**
     * Window Offset:
     * 默认窗口都是基于整点开始的，我们可以通过offset来自定义窗口的开始时间
     */
    tabEnv.executeSql(
      """
        |SELECT window_start, window_end, SUM(price)
        |  FROM TABLE(
        |    TUMBLE(TABLE Bid, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES, INTERVAL '1' MINUTES))
        |  GROUP BY window_start, window_end
        |""".stripMargin)
      .print()

    /**
     * Grouping sets
     *
     * +----+-------------------------+-------------------------+--------------------------------+--------------------------------+
     * | op |            window_start |              window_end |                       supplier |                          price |
     * +----+-------------------------+-------------------------+--------------------------------+--------------------------------+
     * | +I | 2022-04-15 08:00:00.000 | 2022-04-15 08:10:00.000 |                      supplier1 |                            6.0 |
     * | +I | 2022-04-15 08:10:00.000 | 2022-04-15 08:20:00.000 |                      supplier1 |                            1.0 |
     * | +I | 2022-04-15 08:00:00.000 | 2022-04-15 08:10:00.000 |                      supplier2 |                            5.0 |
     * | +I | 2022-04-15 08:10:00.000 | 2022-04-15 08:20:00.000 |                      supplier2 |                            9.0 |
     * | +I | 2022-04-15 08:00:00.000 | 2022-04-15 08:10:00.000 |                         (NULL) |                           11.0 |
     * | +I | 2022-04-15 08:10:00.000 | 2022-04-15 08:20:00.000 |                         (NULL) |                           10.0 |
     * +----+-------------------------+-------------------------+--------------------------------+--------------------------------+
     */
    tabEnv.executeSql(
      """
        |SELECT
        |  window_start
        |, window_end
        |, supplier
        |, SUM(price) as price
        |FROM TABLE(
        | TUMBLE(TABLE Bid, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES)
        |)
        |GROUP BY window_start, window_end,
        |GROUPING SETS ((supplier), ())
        |""".stripMargin)
      .print()

    /**
     * Window Time
     *
     * 我们可以使用 window_time 作为 time attribute 参与下一次的窗口计算
     *
     *
     * +----+-------------------------+-------------------------+-------------------------+--------------------------------+
     * | op |            window_start |              window_end |                 rowtime |                  partial_price |
     * +----+-------------------------+-------------------------+-------------------------+--------------------------------+
     * | +I | 2022-04-15 08:05:00.000 | 2022-04-15 08:10:00.000 | 2022-04-15 08:09:59.999 |                            6.0 |
     * | +I | 2022-04-15 08:10:00.000 | 2022-04-15 08:15:00.000 | 2022-04-15 08:14:59.999 |                            1.0 |
     * | +I | 2022-04-15 08:05:00.000 | 2022-04-15 08:10:00.000 | 2022-04-15 08:09:59.999 |                            5.0 |
     * | +I | 2022-04-15 08:10:00.000 | 2022-04-15 08:15:00.000 | 2022-04-15 08:14:59.999 |                            3.0 |
     * | +I | 2022-04-15 08:15:00.000 | 2022-04-15 08:20:00.000 | 2022-04-15 08:19:59.999 |                            6.0 |
     * +----+-------------------------+-------------------------+-------------------------+--------------------------------+
     */
    tabEnv.executeSql(
      """
        |SELECT
        |  window_start
        |, window_end
        |, window_time as rowtime
        |, SUM(price) as partial_price
        |FROM TABLE(
        |    TUMBLE(TABLE Bid, DESCRIPTOR(rowtime), INTERVAL '5' MINUTES))
        |GROUP BY supplier, window_start, window_end, window_time
        |""".stripMargin)
      .print()
  }

  /**
   * Window Join 示例
   *
   * @param sEnv
   * @param tabEnv
   */
  def windowJoinSample(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val dataStream = sEnv.fromElements(
      Item("L1", 1, LocalDateTime.parse("2022-04-15 12:02:00", formatter).atZone(ZoneId.systemDefault()).toInstant),
      Item("L2", 2, LocalDateTime.parse("2022-04-15 12:06:00", formatter).atZone(ZoneId.systemDefault()).toInstant),
      Item("L3", 3, LocalDateTime.parse("2022-04-15 12:03:00", formatter).atZone(ZoneId.systemDefault()).toInstant)
    )

    tabEnv.createTemporaryView(
      "LeftTable",
      dataStream,
      Schema.newBuilder()
        .columnByExpression("rowtime", "CAST(bidtime AS TIMESTAMP_LTZ(3))")
        .watermark("rowtime", "rowtime - INTERVAL '1' SECOND")
        .build()
    )

    val dataStream2 = sEnv.fromElements(
      Item("L2", 2, LocalDateTime.parse("2022-04-15 12:01:00", formatter).atZone(ZoneId.systemDefault()).toInstant),
      Item("L3", 3, LocalDateTime.parse("2022-04-15 12:04:00", formatter).atZone(ZoneId.systemDefault()).toInstant),
      Item("L4", 4, LocalDateTime.parse("2022-04-15 12:05:00", formatter).atZone(ZoneId.systemDefault()).toInstant)
    )

    tabEnv.createTemporaryView(
      "RightTable",
      dataStream2,
      Schema.newBuilder()
        .columnByExpression("rowtime", "CAST(bidtime AS TIMESTAMP_LTZ(3))")
        .watermark("rowtime", "rowtime - INTERVAL '1' SECOND")
        .build()
    )

    /**
     * Join
     *
     * +----+-------------+--------------------------------+-------------+--------------------------------+-------------------------+-------------------------+
     * | op |       L_Num |                           L_Id |       R_Num |                           R_Id |            window_start |              window_end |
     * +----+-------------+--------------------------------+-------------+--------------------------------+-------------------------+-------------------------+
     * | +I |           3 |                             L3 |           3 |                             L3 | 2022-04-15 12:00:00.000 | 2022-04-15 12:05:00.000 |
     * | +I |      (NULL) |                         (NULL) |           4 |                             L4 | 2022-04-15 12:05:00.000 | 2022-04-15 12:10:00.000 |
     * | +I |           1 |                             L1 |      (NULL) |                         (NULL) | 2022-04-15 12:00:00.000 | 2022-04-15 12:05:00.000 |
     * | +I |      (NULL) |                         (NULL) |           2 |                             L2 | 2022-04-15 12:00:00.000 | 2022-04-15 12:05:00.000 |
     * | +I |           2 |                             L2 |      (NULL) |                         (NULL) | 2022-04-15 12:05:00.000 | 2022-04-15 12:10:00.000 |
     * +----+-------------+--------------------------------+-------------+--------------------------------+-------------------------+-------------------------+
     */
    tabEnv.executeSql(
      """
        |SELECT
        |  L.num as L_Num, L.id as L_Id
        |, R.num as R_Num, R.id as R_Id
        |, coalesce(L.window_start, R.window_start) as window_start
        |, coalesce(L.window_end, R.window_end) as window_end
        |FROM (
        | SELECT
        |  *
        | FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTES))
        |) L
        |FULL JOIN (
        | SELECT
        |  *
        | FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTES))
        |) R
        |ON L.num = R.num AND L.window_start = R.window_start AND L.window_end = R.window_end
        |
        |""".stripMargin)
      .print()
  }

  case class Bid(item: String, price: Float, supplier: String, bidtime: Instant)

  case class Item(id: String, num: Int, bidtime: Instant)

}
