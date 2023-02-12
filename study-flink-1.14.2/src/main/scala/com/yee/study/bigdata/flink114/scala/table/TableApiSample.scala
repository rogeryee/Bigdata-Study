package com.yee.study.bigdata.flink114.scala.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Instant

/**
 * Table Api 示例
 *
 * @author Roger.Yi
 */
object TableApiSample {

  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(sEnv)

    // Scan, Projection, Filter 示例
    //    scanSamples(sEnv, tEnv)

    // Column Operation 示例
    //    columnOperationSamples(sEnv, tEnv)

    // Aggregation 示例
    aggregationSamples(sEnv, tEnv)
  }

  /**
   * Scan, Projection, Filter 示例
   *
   * @param sEnv
   * @param tabEnv
   * @throws Exception
   */
  @throws[Exception]
  def scanSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    // DataStream
    val dataStream = sEnv.fromElements(
      Order("Roger", "China", "Shanghai", Instant.ofEpochMilli(1000)),
      Order("Andy", "China", "Shanghai", Instant.ofEpochMilli(1000)),
      Order("John", "USA", "NY", Instant.ofEpochMilli(1000)),
      Order("Eric", "USA", "LA", Instant.ofEpochMilli(1000)),
      Order("Rooney", "UK", "MAN", Instant.ofEpochMilli(1000))
    )

    tabEnv.createTemporaryView("Orders", dataStream)
    val orders = tabEnv.from("Orders") // schema (a, b, c, rowtime)
    /**
     * 1) Overview & Samples
     * +----+--------------------------------+----------------------+
     * | op |                              a |                  cnt |
     * +----+--------------------------------+----------------------+
     * | +I |                          Roger |                    1 |
     * | +I |                         Rooney |                    1 |
     * | +I |                           Andy |                    1 |
     * | +I |                           John |                    1 |
     * | +I |                           Eric |                    1 |
     * +----+--------------------------------+----------------------+
     */
    orders
      .groupBy($"a")
      .select($"a", $"b".count as "cnt")
      .execute()
      .print()

    /**
     * 2) Select
     * +----+--------------------------------+--------------------------------+
     * | op |                              a |                              d |
     * +----+--------------------------------+--------------------------------+
     * | +I |                          Roger |                       Shanghai |
     * | +I |                           Andy |                       Shanghai |
     * | +I |                           John |                             NY |
     * | +I |                           Eric |                             LA |
     * | +I |                         Rooney |                            MAN |
     * +----+--------------------------------+--------------------------------+
     */
    orders
      .select($"a", $"c" as "d")
      .execute()
      .print()

    /**
     * 3) As
     * +----+----------+----------+-------------+-------------------------------+
     * | op |        x |        y |           z |                             t |
     * +----+----------+----------+-------------+-------------------------------+
     * | +I |    Roger |    China |    Shanghai | 1970-01-01 08:00:01.000000000 |
     * | +I |     Andy |    China |    Shanghai | 1970-01-01 08:00:01.000000000 |
     * | +I |     John |      USA |          NY | 1970-01-01 08:00:01.000000000 |
     * | +I |     Eric |      USA |          LA | 1970-01-01 08:00:01.000000000 |
     * | +I |   Rooney |       UK |         MAN | 1970-01-01 08:00:01.000000000 |
     * +----+----------+----------+-------------+-------------------------------+
     */
    orders
      .as("x, y, z, t")
      .execute
      .print()

    /**
     * 4) Where/Filter
     *
     * +----+--------+--------+-----------+-------------------------------+
     * | op |      a |      b |         c |                       rowtime |
     * +----+--------+--------+-----------+-------------------------------+
     * | +I |  Roger |  China |  Shanghai | 1970-01-01 08:00:01.000000000 |
     * | +I |   Andy |  China |  Shanghai | 1970-01-01 08:00:01.000000000 |
     * +----+--------+--------+-----------+-------------------------------+
     */
    orders
      .filter($"b" === "China")
      .execute
      .print()
  }

  /**
   * Column Operator 示例
   *
   * @param sEnv
   * @param tabEnv
   * @throws Exception
   */
  def columnOperationSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    // DataStream
    val dataStream = sEnv.fromElements(
      Order("Roger", "China", "Shanghai", Instant.ofEpochMilli(1000)),
      Order("Andy", "China", "Shanghai", Instant.ofEpochMilli(1000)),
      Order("John", "USA", "NY", Instant.ofEpochMilli(1000)),
      Order("Eric", "USA", "LA", Instant.ofEpochMilli(1000)),
      Order("Rooney", "UK", "MAN", Instant.ofEpochMilli(1000)));

    tabEnv.createTemporaryView(
      "Orders",
      dataStream);
    val orders = tabEnv.from("Orders"); // schema (a, b, c, rowtime)

    /**
     * 1) AddColumns
     * +----+----------+---------+------------+-------------------------------+-------------------+
     * | op |        a |       b |          c |                       rowtime |              desc |
     * +----+----------+---------+------------+-------------------------------+-------------------+
     * | +I |    Roger |   China |   Shanghai | 1970-01-01 08:00:01.000000000 |     Shanghaisunny |
     * | +I |     Andy |   China |   Shanghai | 1970-01-01 08:00:01.000000000 |     Shanghaisunny |
     * | +I |     John |     USA |         NY | 1970-01-01 08:00:01.000000000 |           NYsunny |
     * | +I |     Eric |     USA |         LA | 1970-01-01 08:00:01.000000000 |           LAsunny |
     * | +I |   Rooney |      UK |        MAN | 1970-01-01 08:00:01.000000000 |          MANsunny |
     * +----+----------+---------+------------+-------------------------------+-------------------+
     */
    orders
      .addColumns(concat($"c", "sunny") as "desc")
      .execute()
      .print();

    /**
     * 2) AddOrReplaceColumns
     */
    orders
      .addOrReplaceColumns(concat($"c", "sunny") as "desc")
      .execute()
      .print();

    /**
     * 3) DropColumns
     * +----+--------------------------------+-------------------------------+
     * | op |                              a |                       rowtime |
     * +----+--------------------------------+-------------------------------+
     * | +I |                          Roger | 1970-01-01 08:00:01.000000000 |
     * | +I |                           Andy | 1970-01-01 08:00:01.000000000 |
     * | +I |                           John | 1970-01-01 08:00:01.000000000 |
     * | +I |                           Eric | 1970-01-01 08:00:01.000000000 |
     * | +I |                         Rooney | 1970-01-01 08:00:01.000000000 |
     * +----+--------------------------------+-------------------------------+
     */
    orders
      .dropColumns($"b", $"c")
      .execute()
      .print();

    /**
     * 4) RenameColumns
     * +----+--------+-------+----------+-------------------------------+
     * | op |      a |    b2 |       c2 |                       rowtime |
     * +----+--------+-------+----------+-------------------------------+
     * | +I |  Roger | China | Shanghai | 1970-01-01 08:00:01.000000000 |
     * | +I |   Andy | China | Shanghai | 1970-01-01 08:00:01.000000000 |
     * | +I |   John |   USA |       NY | 1970-01-01 08:00:01.000000000 |
     * | +I |   Eric |   USA |       LA | 1970-01-01 08:00:01.000000000 |
     * | +I | Rooney |    UK |      MAN | 1970-01-01 08:00:01.000000000 |
     * +----+--------+-------+----------+-------------------------------+
     */
    orders
      .renameColumns($"b" as "b2", $"c" as "c2")
      .execute()
      .print();
  }

  /**
   * Aggregation 示例
   *
   * @param sEnv
   * @param tabEnv
   * @throws Exception
   */
  def aggregationSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    // DataStream
    val dataStream = sEnv.fromElements(
      Order("Roger", "China", "Shanghai", Instant.ofEpochMilli(1000)),
      Order("Andy", "China", "Shanghai", Instant.ofEpochMilli(1000)),
      Order("John", "USA", "NY", Instant.ofEpochMilli(1000)),
      Order("Eric", "USA", "LA", Instant.ofEpochMilli(1000)),
      Order("Rooney", "UK", "MAN", Instant.ofEpochMilli(1000)))
    tabEnv.createTemporaryView("Orders", dataStream)
    val orders = tabEnv.from("Orders") // schema (a, b, c, rowtime)

    /**
     * 1) GroupBy
     * +----+--------------------------------+----------------------+
     * | op |                              a |                    d |
     * +----+--------------------------------+----------------------+
     * | +I |                           Eric |                    1 |
     * | +I |                          Roger |                    1 |
     * | +I |                         Rooney |                    1 |
     * | +I |                           Andy |                    1 |
     * | +I |                           John |                    1 |
     * +----+--------------------------------+----------------------+
     */
    orders
      .groupBy($"b")
      .select($"b", $"a".count as "d")
      .execute
      .print
  }
}

case class Order(a: String, b: String, c: String, rowtime: Instant)