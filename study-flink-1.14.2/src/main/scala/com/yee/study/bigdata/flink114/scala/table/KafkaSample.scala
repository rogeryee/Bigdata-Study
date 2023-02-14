package com.yee.study.bigdata.flink114.scala.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Flink SQL + Kafka 示例
 *
 * Flink 从 Kafka 中消费了用户行为数据，处理后将 PV 和 UV 输出到 MySQL
 *
 * @author Roger.Yi
 */
object KafkaSample {

  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(sEnv)

    // Kafka Source
    tEnv.executeSql(
      """
        |CREATE TABLE kafka_table (
        | `user_id` BIGINT,
        | `item_id` BIGINT,
        | `category_id` BIGINT,
        | `behavior_time` TIMESTAMP(3),
        | `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
        | `offset` BIGINT METADATA VIRTUAL,
        | `partition` BIGINT METADATA VIRTUAL
        |) WITH (
        | 'connector' = 'kafka',
        | 'topic' = 'user-behavior',
        | 'properties.bootstrap.servers' = 'localhost:9092',
        | 'properties.group.id' = 'test_group',
        | 'scan.startup.mode' = 'earliest-offset',
        | 'format' = 'json',
        | 'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin)

    //    sinkDetail(sEnv, tEnv)
    sinkStat(sEnv, tEnv)
    sEnv.execute()
  }

  def sinkDetail(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    // Sink Table
    /**
     * MySQL Table DDL:
     * create table `flink`.`user_behavior`
     * (
     * `user_id`       BIGINT      not null,
     * `item_id`       BIGINT      not null,
     * `category_id`   BIGINT      not null,
     * `behavior_time` DATETIME,
     * `event_time`    DATETIME,
     * primary key (`user_id`, `item_id`)
     * ) engine = InnoDB
     * comment = 'user_behavior';
     */
    tabEnv.executeSql(
      """
        |CREATE TABLE user_behavior_flink (
        | `user_id` BIGINT,
        | `item_id` BIGINT,
        | `category_id` BIGINT,
        | `behavior_time` TIMESTAMP(3),
        | `event_time` TIMESTAMP(3),
        | PRIMARY KEY (`user_id`, `behavior_time`) NOT ENFORCED
        |) WITH (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://localhost:3306/flink?useSSL=false',
        | 'driver' = 'com.mysql.cj.jdbc.Driver',
        | 'table-name' = 'user_behavior',
        | 'username' = 'root',
        | 'password' = '12345678'
        |)
        |""".stripMargin)

    // Sink
    tabEnv.executeSql(
      """
        |INSERT INTO user_behavior_flink
        |SELECT `user_id`, `item_id`, `category_id`, `behavior_time`, `event_time`
        |FROM kafka_table
        |""".stripMargin)
  }

  def sinkStat(sEnv: StreamExecutionEnvironment, tabEnv: StreamTableEnvironment): Unit = {
    val statTable = tabEnv.sqlQuery(
      """
        |SELECT
        |  DATE_FORMAT(behavior_time, 'yyyy-MM-dd') as stat_date
        |, count(1) as pv
        |, count(distinct user_id) as uv
        |FROM kafka_table
        |GROUP BY DATE_FORMAT(behavior_time, 'yyyy-MM-dd')
        |""".stripMargin)
    statTable.execute().print()

    tabEnv.createTemporaryView("stat_table", statTable)

    // Sink Table
    tabEnv.executeSql(
      """
        |CREATE TABLE user_behavior_stat_flink (
        | `stat_date` STRING,
        | `pv` BIGINT,
        | `uv` BIGINT,
        | PRIMARY KEY (`stat_date`) NOT ENFORCED
        |) WITH (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://localhost:3306/flink?useSSL=false',
        | 'driver' = 'com.mysql.cj.jdbc.Driver',
        | 'table-name' = 'user_behavior_stat',
        | 'username' = 'root',
        | 'password' = '12345678'
        |)
        |""".stripMargin)

    // Sink
    tabEnv.executeSql(
      """
        |INSERT INTO user_behavior_stat_flink
        |SELECT `stat_date`, `pv`, `uv`
        |FROM stat_table
        |""".stripMargin)
  }
}
