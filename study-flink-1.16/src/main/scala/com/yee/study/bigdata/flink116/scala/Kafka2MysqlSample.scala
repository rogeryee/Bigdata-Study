package com.yee.study.bigdata.flink116.scala

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}

/**
 * 基于 flink sql 消费 Kafka 消息并写入 mysql 的示例
 *
 * @author Roger.Yi
 */
object Kafka2MysqlSample {

  def main(args: Array[String]): Unit = {
    // Create Env
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(sEnv)

    System.out.println("Table Env created....")

    /**
     * Kafka 消息
     *
     * 创建 Topic ：./kafka-topics.sh --create --topic user-order --bootstrap-server localhost:9092
     *
     * 生产者：./kafka-console-producer.sh --topic user-order --bootstrap-server localhost:9092
     * 消费者：./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user-order
     *
     * {"user_id": 1, "item_id": 1, "category_id": 1, "behavior_time_ts": 1677640151580}
     * {"user_id": 2, "item_id": 2, "category_id": 1, "behavior_time_ts": 1677640151590}
     * {"user_id": 3, "item_id": 3, "category_id": 2, "behavior_time_ts": 1677640151680}
     * {"user_id": 1, "item_id": 1, "category_id": 1, "behavior_time_ts": 1677640152580}
     */
    tableEnv.executeSql(
      """
        |CREATE TABLE kafka_source (
        | `user_id` BIGINT,
        | `item_id` BIGINT,
        | `category_id` BIGINT,
        | `behavior_time_ts` BIGINT,
        | `behavior_time` AS TO_TIMESTAMP_LTZ(`behavior_time_ts`, 3),
        | `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
        | `offset` BIGINT METADATA VIRTUAL,
        | `partition` BIGINT METADATA VIRTUAL
        |) WITH (
        | 'connector' = 'kafka',
        | 'properties.bootstrap.servers' = 'localhost:9092',
        | 'properties.enable.auto.commit' = 'true',
        | 'properties.auto.commit.interval.ms' = '1000',
        | 'properties.group.id' = 'flink_group',
        | 'topic' = 'user-order',
        | 'scan.startup.mode' = 'latest-offset',
        | 'format' = 'json',
        | 'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin)

    val statTable: Table = tableEnv.sqlQuery(
      """
        |SELECT
        |  DATE_FORMAT(behavior_time, 'yyyy-MM-dd') as stat_date
        |, category_id
        |, count(1) as pv
        |, count(distinct user_id) as uv
        |FROM kafka_source
        |GROUP BY DATE_FORMAT(behavior_time, 'yyyy-MM-dd'), category_id
        |""".stripMargin)
    tableEnv.createTemporaryView("stat_table", statTable)

    // Print to console
    val dataStream = tableEnv.toChangelogStream(statTable)
    dataStream.print()

    /**
     * MySQL 表
     * CREATE TABLE flink.`user_order_stat` (
     * `stat_date` varchar(20) NOT NULL,
     * `category_id` bigint NOT NULL,
     * `pv` bigint(20) DEFAULT NULL,
     * `uv` bigint(20) DEFAULT NULL,
     * PRIMARY KEY (`stat_date`,`category_id`)
     * ) ENGINE=InnoDB;
     */
    // Sink Table
    tableEnv.executeSql(
      """
        |CREATE TABLE user_order_stat_sink (
        | `stat_date` STRING,
        | `category_id` BIGINT,
        | `pv` BIGINT,
        | `uv` BIGINT,
        | PRIMARY KEY (`stat_date`, `category_id`) NOT ENFORCED
        |) WITH (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://localhost:3306/flink?useSSL=false',
        | 'driver' = 'com.mysql.cj.jdbc.Driver',
        | 'table-name' = 'user_order_stat',
        | 'username' = 'root',
        | 'password' = '12345678'
        |)
        |""".stripMargin)

    // Sink
    tableEnv.executeSql(
      """
        |INSERT INTO user_order_stat_sink
        |SELECT `stat_date`, `category_id`, `pv`, `uv`
        |FROM stat_table
        |""".stripMargin)

    sEnv.execute()
  }
}
