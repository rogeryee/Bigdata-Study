package com.yee.study.bigdata.flink116.scala

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Instant

/**
 *
 * @author Roger.Yi
 */
object TableApiSample {

  def main(args: Array[String]): Unit = {
    //    sample_1() // 基本 Table Api 使用框架
    sample_2() // fromDataStream 示例
  }

  /**
   * 基本 Table Api 使用框架
   *
   * @param tableEnv
   */
  def sample_1(): Unit = {
    // Create Env
    val settings = EnvironmentSettings
      .newInstance()
      .build()
    val tableEnv = TableEnvironment.create(settings)

    // Create a source table
    tableEnv.createTemporaryTable("SourceTable",
      TableDescriptor.forConnector("datagen")
        .schema(
          Schema
            .newBuilder()
            .column("f0", DataTypes.STRING())
            .build())
        .option("number-of-rows", "3")
        .build())

    // Create a sink table (using SQL DDL)
    tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'print') LIKE SourceTable (EXCLUDING OPTIONS) ")

    // Create a Table object from a Table API query
    val table1 = tableEnv.from("SourceTable")

    // Create a Table object from a SQL query
    val table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable")

    // Emit a Table API result Table to a TableSink, same for SQL result
    val tableResult = table1.insertInto("SinkTable").execute().print()
  }

  /**
   * fromDataStream 示例
   *
   * @param sEnv
   * @param tEnv
   * @return
   */
  def sample_2(): Unit = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(sEnv)

    val ds = sEnv.fromElements(
      User("Roger", 10, Instant.ofEpochMilli(1000)),
      User("Andy", 3, Instant.ofEpochMilli(1001)),
      User("John", 6, Instant.ofEpochMilli(1002)))

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
  }

  case class User(name: String, score: java.lang.Integer, event_time: java.time.Instant)

}
