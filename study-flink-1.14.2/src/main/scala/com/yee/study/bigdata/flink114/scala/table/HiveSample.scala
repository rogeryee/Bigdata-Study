package com.yee.study.bigdata.flink114.scala.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{SqlDialect, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * Flink SQL + Kafka 示例
 *
 * Flink 从 Kafka 中消费了用户行为数据，处理后将 PV 和 UV 输出到 Hive
 *
 * ./zookeeper-server-start.sh ../config/zookeeper.properties
 * ./kafka-server-start.sh ../config/server.properties
 * ./kafka-console-producer.sh --topic user-behavior --bootstrap-server localhost:9092
 *
 * {"user_id": 1, "item_id": 1, "category_id": 1, "behavior_time_ts": 1677640151580}
 *
 * @author Roger.Yi
 */
object HiveSample {

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
        | `behavior_time_ts` BIGINT,
        | `behavior_time` AS TO_TIMESTAMP_LTZ(`behavior_time_ts`, 3),
        | `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
        | `offset` BIGINT METADATA VIRTUAL,
        | `partition` BIGINT METADATA VIRTUAL
        |) WITH (
        | 'connector' = 'kafka',
        | 'topic' = 'user-behavior',
        | 'properties.bootstrap.servers' = 'localhost:9092',
        | 'properties.group.id' = 'test_group',
        | 'properties.enable.auto.commit' = 'true',
        | 'properties.auto.commit.interval.ms' = '1000',
        | 'scan.startup.mode' = 'latest-offset',
        | 'format' = 'json',
        | 'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin)

    sinkDetail(sEnv, tEnv)
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
    tabEnv.useCatalog("default_catalog")
    tabEnv.useDatabase("default_database")
    val statTable: Table = tabEnv.sqlQuery(
      """
        |SELECT
        |  DATE_FORMAT(behavior_time, 'yyyy-MM-dd') as stat_date
        |, user_id as pv
        |, item_id as uv
        |FROM kafka_table
        |""".stripMargin)
    tabEnv.createTemporaryView("stat_table", statTable)

    val hive: HiveCatalog = new HiveCatalog(
      "myhive",
      "roger_tmp",
      "/Users/cntp/MyWork/DevTools/apache-hive-3.1.3-bin/conf")
    tabEnv.registerCatalog("myhive", hive)
    tabEnv.useCatalog("myhive")
    tabEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    // Sink Table
    tabEnv.executeSql(
      """
        |CREATE TABLE IF NOT EXISTS user_behavior_stat_flink (
        | `pv` BIGINT,
        | `uv` BIGINT
        |)
        |PARTITIONED BY (`stat_date` STRING)
        |STORED AS parquet TBLPROPERTIES
        |(
        | 'auto-compaction' = 'true',
        | 'compaction.file-size' = '128MB',
        | 'format' = 'parquet',
        | 'parquet.compression' = 'GZIP',
        | 'sink.partition-commit.policy.kind'='metastore,success-file'
        |)
        |""".stripMargin)

    // Sink
    tabEnv.executeSql(
      """
        |INSERT INTO myhive.roger_tmp.user_behavior_stat_flink
        |SELECT `pv`, `uv`, `stat_date`
        |FROM default_catalog.default_database.stat_table
        |""".stripMargin)
  }
}
