package com.yee.study.bigdata.flink114.scala.table

import com.yee.study.bigdata.flink114.java.table.{User, UserGenericType}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Expressions.row
import org.apache.flink.table.api.bridge.scala.{StreamStatementSet, StreamTableEnvironment}
import org.apache.flink.table.api.{DataTypes, FieldExpression, Schema, Table, _}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.Collector

import java.time.Instant

/**
 * DataStream 和 Table 配合使用的示例2 (基于Scala)
 *
 * @author Roger.Yil
 */
object DataStreamApiSample {

  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(sEnv)

    // fromDataStream 示例
    //    fromDataStreamSamples(sEnv, tEnv)

    // createTemporaryVie 示例
    //    createTemporaryViewSamples(sEnv, tEnv)

    // toDataStream 示例
    //    toDataStreamSamples(sEnv, tEnv)

    // fromChangelogStream 示例
    //    fromChangelogStreamSamples(sEnv, tEnv)

    // fromChangelogStream 示例
    //    toChangelogStreamSamples(sEnv, tEnv)

    // StreamStatementSet 示例
//    streamStatementSetSamples(sEnv, tEnv)

    // Time Attributes （EventTime、ProcessTime） 示例
    timeAttributesSamples(sEnv, tEnv)
  }

  /**
   * fromDataStream 示例
   *
   * @param sEnv
   * @param tEnv
   * @return
   */
  def fromDataStreamSamples(sEnv: StreamExecutionEnvironment, tEnv: StreamTableEnvironment): Unit = {
    val ds = sEnv.fromElements(
      new User("Roger", 10, Instant.ofEpochMilli(1000)),
      new User("Andy", 3, Instant.ofEpochMilli(1001)),
      new User("John", 6, Instant.ofEpochMilli(1002)))

    /**
     * 1) fromDataStream(DataStream)
     * 根据自定义类型获取字段
     * (
     * `name` STRING,
     * `score` INT NOT NULL,
     * `event_time` TIMESTAMP_LTZ(9)
     * )
     */
    val table1 = tEnv.fromDataStream(ds)
    table1.printSchema()

    /**
     * 2) fromDataStream(DataStream, Schema)
     * 自动获取字段类型、并增加一个计算字段（基于proctime）
     * (
     * `name` STRING,
     * `score` INT NOT NULL,
     * `event_time` TIMESTAMP_LTZ(9),
     * `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
     * )
     */
    val table2 = tEnv.fromDataStream(
      ds,
      Schema.newBuilder
        .columnByExpression("proc_time", "PROCTIME()")
        .build
    )
    table2.printSchema()

    /**
     * 3) fromDataStream(DataStream, Schema)
     * 自动获取字段类型、并增加一个计算字段（基于proctime）和一个自定义的watermark策略
     * (
     * `name` STRING,
     * `score` INT NOT NULL,
     * `event_time` TIMESTAMP_LTZ(9),
     * `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
     * WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
     * )
     */
    val table3 = tEnv.fromDataStream(
      ds,
      Schema.newBuilder
        .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
        .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
        .build
    )
    table3.printSchema()

    /**
     * 4) fromDataStream(DataStream, Schema)
     * 手动定义字段类型
     * (
     * `name` STRING,
     * `score` INT NOT NULL,
     * `event_time` TIMESTAMP_LTZ(9),
     * `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
     * WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
     * )
     */
    val table4 = tEnv.fromDataStream(
      ds,
      Schema.newBuilder
        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
        .watermark("rowtime", "SOURCE_WATERMARK()")
        .build
    )
    table4.printSchema()

    // DataStream with GenericType
    val ds2 = sEnv.fromElements(
      new UserGenericType("Roger", 10),
      new UserGenericType("Andy", 3),
      new UserGenericType("John", 6))

    /**
     * 5) fromDataStream(DataStream)
     * RAW type，默认字段名为 f0
     * (
     * `f0` RAW('com.yee.study.bigdata.flink114.table.UserGenericType', '...')
     * )
     */
    val table5 = tEnv.fromDataStream(ds2)
    table5.printSchema()

    /**
     * 6) fromDataStream(DataStream, Schema)
     * 使用 as 方法自定义类型名
     * (
     * `user` *com.yee.study.bigdata.flink114.table.UserGenericType<`name` STRING, `score` INT NOT NULL>*
     * )
     */
    val table6 = tEnv.fromDataStream(
      ds2,
      Schema.newBuilder
        .column("f0", DataTypes.of(classOf[UserGenericType]))
        .build
    ).as("user")
    table6.printSchema()

    /**
     * 7) fromDataStream(DataStream, Schema)
     * 自定义的指定类型
     * (
     * `user` *com.yee.study.bigdata.flink114.table.UserGenericType<`name` STRING, `score` INT>*
     * )
     */
    val table7 = tEnv.fromDataStream(
      ds2,
      Schema.newBuilder
        .column(
          "f0",
          DataTypes.STRUCTURED(
            classOf[UserGenericType],
            DataTypes.FIELD("name", DataTypes.STRING),
            DataTypes.FIELD("score", DataTypes.INT)))
        .build
    ).as("user")
    table7.printSchema()
  }

  /**
   * createTemporaryView 示例
   *
   * @param sEnv
   * @param tabEnv
   */
  def createTemporaryViewSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = { // DataStream
    val dataStream = sEnv.fromElements(
      Tuple2(12L, "Alice"),
      Tuple2(0L, "Bob"))

    /**
     * 1) 自动获取字段
     * (
     * `_1` BIGINT NOT NULL,
     * `_2` STRING
     * )
     */
    tabEnv.createTemporaryView("MyView", dataStream)
    tabEnv.from("MyView").printSchema()

    /**
     * 2) 自定义字段
     * (
     * `_1` BIGINT,
     * `_2` STRING
     * )
     */
    tabEnv.createTemporaryView(
      "MyView2",
      dataStream,
      Schema.newBuilder
        .column("_1", "BIGINT")
        .column("_2", "STRING")
        .build
    )
    tabEnv.from("MyView2").printSchema()

    /**
     * 3) 基于Table创建
     * (
     * `id` BIGINT NOT NULL,
     * `name` STRING
     * )
     */
    tabEnv.createTemporaryView(
      "MyView3",
      tabEnv
        .fromDataStream(dataStream)
        .as("id", "name"))
    tabEnv.from("MyView3").printSchema()
  }

  /**
   * toDataStream 示例
   *
   * @param sEnv
   * @param tabEnv
   */
  def toDataStreamSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    // Table
    tabEnv.executeSql(
      """
        |CREATE TABLE GeneratedTable
        |(
        |   name STRING,
        |   score INT,
        |   event_time TIMESTAMP_LTZ(3),
        |   WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        |)
        |WITH (
        | 'connector' = 'datagen',
        | 'number-of-rows' = '3'
        |)
        |""".stripMargin)
    val table: Table = tabEnv.from("GeneratedTable")

    /**
     * ROW 类型
     * 1> +I[cfa5dfab5116817ff7bacb07315558d12b85917a2f59a662f4c83e7c5615d02f1f75dc9d9510cd0e55a91a227dae4aa485ec, 1014879561, 2023-02-10T05:16:54.130Z]
     * 2> +I[cde737f4cea803c822c69b6942eceec430802852fc0766b4a0c2d91c4c02538b68c6691cc10662e08b49eb6ca59513849cf9, 569986090, 2023-02-10T05:16:54.130Z]
     */
    val dataStream: DataStream[Row] = tabEnv.toDataStream(table)
    dataStream.print()

    /**
     * User 类型
     * 10> com.yee.study.bigdata.flink114.java.table.User
     * 7> com.yee.study.bigdata.flink114.java.table.User
     */
    val dataStream2: DataStream[User] = tabEnv.toDataStream(table, classOf[User])
    dataStream2.print();

    sEnv.execute("toDataStreamSamples");
  }

  /**
   * fromChangelogStream 示例
   *
   * @param sEnv
   * @param tabEnv
   */
  def fromChangelogStreamSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    /**
     * +----+--------------------------------+-------------+
     * | op |                           name |       score |
     * +----+--------------------------------+-------------+
     * | +I |                            Bob |           5 |
     * | +I |                          Alice |          12 |
     * | -D |                          Alice |          12 |
     * | +I |                          Alice |         100 |
     * +----+--------------------------------+-------------+
     */
    val dataStream1: DataStream[Row] = sEnv.fromElements(
      Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
      Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
      Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", Int.box(12)),
      Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
    )(Types.ROW(Types.STRING, Types.INT))
    val table1: Table = tabEnv.fromChangelogStream(dataStream1)
    tabEnv.createTemporaryView("InputTable1", table1)
    tabEnv.executeSql("select f0 as name, sum(f1) as score from InputTable1 group by f0").print();

    /**
     * +----+--------------------------------+-------------+
     * | op |                           name |      EXPR$1 |
     * +----+--------------------------------+-------------+
     * | +I |                            Bob |           5 |
     * | +I |                          Alice |          12 |
     * | -U |                          Alice |          12 |
     * | +U |                          Alice |         100 |
     * +----+--------------------------------+-------------+
     */
    val dataStream2: DataStream[Row] = sEnv.fromElements(
      Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
      Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
      Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
    )(Types.ROW(Types.STRING, Types.INT))
    val table2: Table = tabEnv.fromChangelogStream(
      dataStream2,
      Schema.newBuilder().primaryKey("f0").build(), ChangelogMode.upsert());
    tabEnv.createTemporaryView("InputTable2", table2);
    tabEnv.executeSql("select f0 as name, sum(f1) from InputTable2 group by f0").print();
  }

  /**
   * fromChangelogStream 示例
   *
   * @param sEnv
   * @param tabEnv
   */
  @throws[Exception]
  def toChangelogStreamSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    tabEnv.executeSql(
      """
        |CREATE TABLE GeneratedTable
        |(
        |   name STRING,
        |   score INT,
        |   event_time TIMESTAMP_LTZ(3),
        |   WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        |)
        |WITH (
        | 'connector' = 'datagen',
        | 'number-of-rows' = '3'
        |)
        |""".stripMargin)
    val table: Table = tabEnv.from("GeneratedTable")

    /**
     * +I[Bob, 12]
     * +I[Alice, 12]
     * -U[Alice, 12]
     * +U[Alice, 14]
     */
    val simpleTable: Table = tabEnv.fromValues(
      row("Alice", Int.box(12)),
      row("Alice", Int.box(2)),
      row("Bob", Int.box(12))
    )
      .as("name", "score")
      .groupBy($"name")
      .select($"name", $"score".sum())
    tabEnv.toChangelogStream(simpleTable).executeAndCollect.foreach(System.out.println)

    /**
     * [name, score, event_time]
     * [name, score, event_time]
     */
    val dataStream: DataStream[Row] = tabEnv.toChangelogStream(table)
    dataStream.process(processFunction = new ProcessFunction[Row, Unit] {
      override def processElement(
                                   row: Row,
                                   ctx: ProcessFunction[Row, Unit]#Context,
                                   out: Collector[Unit]): Unit = {

        // prints: [name, score, event_time]
        println(row.getFieldNames(true))

        // timestamp exists twice
        assert(ctx.timestamp() == row.getFieldAs[Instant]("event_time").toEpochMilli)
      }
    })
    sEnv.execute("toChangelogStream")
  }

  /**
   * StreamStatementSet 示例
   *
   * +I[201449218, false]
   * +I[755060145, true]
   * +I[-1345892487, false]
   * +I[1]
   * +I[2]
   * +I[3]
   *
   * @param sEnv
   * @param tabEnv
   * @throws Exception
   */
  def streamStatementSetSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    val sourceDescriptor = TableDescriptor.forConnector("datagen")
      .option("number-of-rows", "3")
      .schema(Schema.newBuilder
        .column("myCol", DataTypes.INT)
        .column("myOtherCol", DataTypes.BOOLEAN).build)
      .build
    val sinkDescriptor = TableDescriptor.forConnector("print").build

    // Add a table pipeline
    val statementSet: StreamStatementSet = tabEnv.createStatementSet
    val tableFromSource: Table = tabEnv.from(sourceDescriptor)
    statementSet.addInsert(sinkDescriptor, tableFromSource)

    // 1, 2, 3
    val dataStream = sEnv.fromElements(1, 2, 3)
    val tableFromStream = tabEnv.fromDataStream(dataStream)
    statementSet.addInsert(sinkDescriptor, tableFromStream)
    statementSet.attachAsDataStream()
    sEnv.execute
  }

  /**
   * Time Attributes 示例
   * 1） EventTime
   * 2） ProcessTime
   *
   * @param sEnv
   * @param tabEnv
   */
  def timeAttributesSamples(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    /**
     * 1）Event Time
     * Schema:
     * (
     *  `user_name` STRING,
     *  `data` STRING,
     *  `user_action_time` TIMESTAMP(3) *ROWTIME*,
     *  WATERMARK FOR `user_action_time`: TIMESTAMP(3) AS `user_action_time` - INTERVAL '5' SECOND
     * )
     *
     * Result：
     * +----+-------------------------+-------------------------+----------------------+
     * | op |                 w_start |                   w_end |                  cnt |
     * +----+-------------------------+-------------------------+----------------------+
     * | +I | 2023-02-13 02:10:54.000 | 2023-02-13 02:10:57.000 |                   24 |
     * | +I | 2023-02-13 02:10:57.000 | 2023-02-13 02:11:00.000 |                   36 |
     * | +I | 2023-02-13 02:11:00.000 | 2023-02-13 02:11:03.000 |                   36 |
     */
    val user_actions_1 = tabEnv.executeSql(
      """
        |CREATE TABLE user_actions_1 (
        | user_name STRING,
        | data STRING,
        | user_action_time TIMESTAMP(3),
        | WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
        |)
        |WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '10'
        |)
        |""".stripMargin)
    tabEnv.from("user_actions_1").printSchema()

    tabEnv.executeSql(
      """
        |SELECT
        |  TUMBLE_START(user_action_time, INTERVAL '3' SECOND) as w_start
        |, TUMBLE_END(user_action_time, INTERVAL '3' SECOND) as w_end
        |, count(1) as cnt
        |FROM user_actions_1
        |GROUP BY TUMBLE(user_action_time, INTERVAL '3' SECOND)
        |""".stripMargin)
//      .print()

    /**
     * 2) ProcessTime
     * Schema:
     * (
     *   `user_name` STRING,
     *   `data` STRING,
     *   `user_action_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
     * )
     *
     * Result:
     * +----+-------------------------+-------------------------+----------------------+
     * | op |                 w_start |                   w_end |                  cnt |
     * +----+-------------------------+-------------------------+----------------------+
     * | +I | 2023-02-13 10:15:54.000 | 2023-02-13 10:15:57.000 |                   24 |
     * | +I | 2023-02-13 10:15:57.000 | 2023-02-13 10:16:00.000 |                   36 |
     * | +I | 2023-02-13 10:16:00.000 | 2023-02-13 10:16:03.000 |                   36 |
     */
    tabEnv.executeSql(
      """
        |CREATE TABLE user_actions_2 (
        | user_name STRING,
        | data STRING,
        | user_action_time AS PROCTIME()
        |)
        |WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '10'
        |)
        |""".stripMargin)
    tabEnv.from("user_actions_2").printSchema()

    tabEnv.executeSql(
      """
        |SELECT
        |  TUMBLE_START(user_action_time, INTERVAL '3' SECOND) as w_start
        |, TUMBLE_END(user_action_time, INTERVAL '3' SECOND) as w_end
        |, count(1) as cnt
        |FROM user_actions_2
        |GROUP BY TUMBLE(user_action_time, INTERVAL '3' SECOND)
        |""".stripMargin)
//      .print()
  }
}