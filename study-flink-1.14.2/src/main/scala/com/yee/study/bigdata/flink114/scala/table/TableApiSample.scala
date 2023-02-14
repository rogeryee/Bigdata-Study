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
        scanSamples(sEnv, tEnv)

    // Column Operation 示例
    //    columnOperationSamples(sEnv, tEnv)

    // Aggregation 示例
    //    aggregationSamples(sEnv, tEnv)

    // Join 示例
    //    joinSamples(sEnv, tEnv)
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
      Order("Roger", "China", "Shanghai", 2000, Instant.now()),
      Order("Andy", "China", "Shanghai", 2000, Instant.now().plusSeconds(2)),
      Order("John", "USA", "NY", 1500, Instant.now().plusSeconds(2)),
      Order("Eric", "USA", "LA", 800, Instant.now().plusSeconds(2)),
      Order("Rooney", "UK", "MAN", 700, Instant.now().plusSeconds(2)))

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
      Order("Roger", "China", "Shanghai", 2000, Instant.now()),
      Order("Andy", "China", "Shanghai", 2000, Instant.now().plusSeconds(2)),
      Order("John", "USA", "NY", 1500, Instant.now().plusSeconds(2)),
      Order("Eric", "USA", "LA", 800, Instant.now().plusSeconds(2)),
      Order("Rooney", "UK", "MAN", 700, Instant.now().plusSeconds(2)))

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
      Order("Roger", "China", "Shanghai", 2000, Instant.now()),
      Order("Andy", "China", "Shanghai", 2000, Instant.now().plusSeconds(2)),
      Order("John", "USA", "NY", 1500, Instant.now().plusSeconds(2)),
      Order("Eric", "USA", "LA", 800, Instant.now().plusSeconds(2)),
      Order("Rooney", "UK", "MAN", 700, Instant.now().plusSeconds(2)))

    tabEnv.createTemporaryView(
      "Orders",
      dataStream,
      Schema.newBuilder()
        .columnByExpression("proc_time", "PROCTIME()")
        .columnByExpression("row_time", "CAST(event_time AS TIMESTAMP_LTZ(3))")
        .watermark("row_time", "row_time - INTERVAL '3' SECOND")
        .build()
    )
    val orders = tabEnv.from("Orders")
    orders.printSchema()

    /**
     * 1) GroupBy
     * +----+--------------------------------+----------------------+
     * | op |                              b |                    d |
     * +----+--------------------------------+----------------------+
     * | +I |                             UK |                    1 |
     * | +I |                            USA |                    1 |
     * | -U |                            USA |                    1 |
     * | +U |                            USA |                    2 |
     * | +I |                          China |                    1 |
     * | -U |                          China |                    1 |
     * | +U |                          China |                    2 |
     * +----+--------------------------------+----------------------+
     */
    orders
      .groupBy($"b")
      .select($"b", $"a".count as "d")
      .execute
    //      .print

    /**
     * EventTime - GroupBy Window Aggregation
     * +----+--------------------------------+----------------------+
     * | op |                              b |                    d |
     * +----+--------------------------------+----------------------+
     * | +I |                             UK |                    1 |
     * | +I |                            USA |                    1 |
     * | -U |                            USA |                    1 |
     * | +U |                            USA |                    2 |
     * | +I |                          China |                    1 |
     * | -U |                          China |                    1 |
     * | +U |                          China |                    2 |
     * +----+--------------------------------+----------------------+
     */
    orders
      .window(Tumble over 3.seconds() on $"row_time" as "w")
      .groupBy($"w")
      .select(
        $"w".start,
        $"w".end,
        $"w".rowtime,
        $"a".count as "d")
      .execute()
    //      .print()

    /**
     * ProcessTime - GroupBy Window Aggregation
     */
    orders
      .window(Tumble over 3.seconds() on $"proc_time" as "w")
      .groupBy($"w")
      .select(
        $"w".start,
        $"w".end,
        $"w".proctime,
        $"a".count as "d")
      .execute()
    //      .print()

    /**
     * Over Window Aggregation
     *
     * +----+---------------+---------------+---------------+---------------+
     * | op |             b |           avg |           max |           min |
     * +----+---------------+---------------+---------------+---------------+
     * | +I |           USA |          1150 |          1500 |           800 |
     * | +I |           USA |          1150 |          1500 |           800 |
     * | +I |            UK |           700 |           700 |           700 |
     * | +I |         China |          2000 |          2000 |          2000 |
     * | +I |         China |          2000 |          2000 |          2000 |
     * +----+---------------+---------------+---------------+---------------+
     */
    orders.window(
      Over
        partitionBy $"b"
        orderBy $"row_time"
        preceding UNBOUNDED_RANGE
        following CURRENT_RANGE
        as "w")
      .select(
        $"b",
        $"d".avg over $"w" as "avg",
        $"d".max over $"w" as "max",
        $"d".min over $"w" as "min"
      )
      .execute()
    //      .print()

    /**
     * Distinct Aggregation
     * +----+--------------------------------+----------------------+
     * | op |                              b |                  cnt |
     * +----+--------------------------------+----------------------+
     * | +I |                             UK |                    1 |
     * | +I |                          China |                    1 |
     * | +I |                            USA |                    1 |
     * | -U |                            USA |                    1 |
     * | +U |                            USA |                    2 |
     * +----+--------------------------------+----------------------+
     */
    orders
      .groupBy($"b")
      .select($"b", $"d".count.distinct as "cnt")
      .execute()
    //      .print()

    /**
     * Distinct
     */
    orders.distinct()
  }

  /**
   * Join 示例
   *
   * @param sEnv
   * @param tabEnv
   */
  def joinSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    // Inner join
    var left = tabEnv.from("MyTable").select($"a", $"b", $"c")
    var right = tabEnv.from("MyTable").select($"d", $"e", $"f")
    var result = left
      .join(right)
      .where($"a" === $"d")
      .select($"a", $"b", $"e")

    // Outer Join
    val leftOuterResult = left
      .leftOuterJoin(right, $"a" === $"d")
      .select($"a", $"b", $"e")
    val rightOuterResult = left
      .rightOuterJoin(right, $"a" === $"d")
      .select($"a", $"b", $"e")
    val fullOuterResult = left
      .fullOuterJoin(right, $"a" === $"d")
      .select($"a", $"b", $"e")

    // Interval Join
    left = tabEnv.from("MyTable").select($"a", $"b", $"c", $"ltime")
    right = tabEnv.from("MyTable").select($"d", $"e", $"f", $"rtime")

    result = left.join(right)
      .where($"a" === $"d" && $"ltime" >= $"rtime" - 5.minutes && $"ltime" < $"rtime" + 10.minutes)
      .select($"a", $"b", $"e", $"ltime")
  }

  /**
   * Set Operations 示例
   *
   * @param sEnv
   * @param tabEnv
   */
  def setOperationsSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    // Union
    var left = tabEnv.from("orders1")
    var right = tabEnv.from("orders2")
    left.union(right)

    // Union All
    left.unionAll(right)

    // Intersect
    left.intersect(right)

    // Intersect All
    left.intersectAll(right)

    // Minus
    left.minus(right)

    // Minus All
    left.minusAll(right)

    // In
    left.select($"a", $"b", $"c").where($"a".in(right))
  }

  /**
   * GroupWindow 示例
   *
   * @param sEnv
   * @param tabEnv
   */
  def groupWindowSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    val input = tabEnv.from("input")

    // Tumble Window
    // Tumbling Event-time Window
    input.window(Tumble over 10.minutes on $"rowtime" as $"w")

    // Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
    input.window(Tumble over 10.minutes on $"proctime" as $"w")

    // Tumbling Row-count Window (assuming a processing-time attribute "proctime")
    input.window(Tumble over 10.rows on $"proctime" as $"w")

    // Slide Window
    // Sliding Event-time Window
    input.window(Slide over 10.minutes every 5.minutes on $"rowtime" as $"w")

    // Sliding Processing-time window (assuming a processing-time attribute "proctime")
    input.window(Slide over 10.minutes every 5.minutes on $"proctime" as $"w")

    // Sliding Row-count window (assuming a processing-time attribute "proctime")
    input.window(Slide over 10.rows every 5.rows on $"proctime" as $"w")

    // Session Window
    // Session Event-time Window
    input.window(Session withGap 10.minutes on $"rowtime" as $"w")

    // Session Processing-time Window (assuming a processing-time attribute "proctime")
    input.window(Session withGap 10.minutes on $"proctime" as $"w")
  }

  /**
   * OverWindow 示例
   *
   * @param sEnv
   * @param tabEnv
   */
  def overWindowSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    val input = tabEnv.from("input")

    // Unbounded Over Windows
    // Unbounded Event-time over window (assuming an event-time attribute "rowtime")
    input.window(Over partitionBy $"a" orderBy $"rowtime" preceding UNBOUNDED_RANGE as "w")

    // Unbounded Processing-time over window (assuming a processing-time attribute "proctime")
    input.window(Over partitionBy $"a" orderBy $"proctime" preceding UNBOUNDED_RANGE as "w")

    // Unbounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
    input.window(Over partitionBy $"a" orderBy $"rowtime" preceding UNBOUNDED_ROW as "w")

    // Unbounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
    input.window(Over partitionBy $"a" orderBy $"proctime" preceding UNBOUNDED_ROW as "w")

    // Bounded Over Windows
    // Bounded Event-time over window (assuming an event-time attribute "rowtime")
    input.window(Over partitionBy $"a" orderBy $"rowtime" preceding 1.minutes as "w")

    // Bounded Processing-time over window (assuming a processing-time attribute "proctime")
    input.window(Over partitionBy $"a" orderBy $"proctime" preceding 1.minutes as "w")

    // Bounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
    input.window(Over partitionBy $"a" orderBy $"rowtime" preceding 10.rows as "w")

    // Bounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
    input.window(Over partitionBy $"a" orderBy $"proctime" preceding 10.rows as "w")

  }

  case class Order(a: String, b: String, c: String, d: Long = 1000L, event_time: Instant)

}