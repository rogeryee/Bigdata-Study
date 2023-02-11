package com.yee.study.bigdata.flink114.scala.table

import com.yee.study.bigdata.flink114.java.table.{User, UserGenericType}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, Schema, Table}

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
    createTemporaryViewSamples(sEnv, tEnv)
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
}