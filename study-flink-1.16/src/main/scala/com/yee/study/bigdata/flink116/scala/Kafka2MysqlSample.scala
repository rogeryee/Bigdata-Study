package com.yee.study.bigdata.flink116.scala

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

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

    /**
     * Kafka 消息
     *
     * 创建 Topic ：./kafka-topics.sh --create --topic order-user --bootstrap-server localhost:9092
     *
     * 生产者：./kafka-console-producer.sh --topic order-user --bootstrap-server localhost:9092
     *
     * 1,Joey,22,SH
     * 2,John,30,SH
     * 3,Roger,20,BJ
     */
    tableEnv.executeSql(
      """
        |create table kafka_order_user (
        |    id STRING,
        |    name STRING,
        |    age integer,
        |    city STRING,
        |    event_time TIMESTAMP(3) METADATA FROM 'timestamp'
        |) with (
        |    'connector' = 'kafka',
        |    'properties.bootstrap.servers' = 'localhost:9092',
        |    'properties.auto.commit.interval.ms' = '1000',
        |    'properties.enable.auto.commit' = 'true',
        |    'properties.group.id' = 'flink_group',
        |    'topic' = 'order-user',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'csv',
        |    'csv.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin)

    /**
     * MySQL 表
     *
     * create table flink.order_user (
     * id varchar(32),
     * name varchar(32),
     * age int,
     * city varchar(32),
     * PRIMARY KEY (`id`)
     * ) ENGINE=InnoDB;
     */
    // Sink Table
    tableEnv.executeSql(
      """
        |CREATE TABLE mysql_order_user (
        |    id STRING,
        |    name STRING,
        |    age INT,
        |    city STRING,
        |    primary key (id) not enforced
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://localhost:3306/flink?useSSL=false',
        |   'driver' = 'com.mysql.cj.jdbc.Driver',
        |   'table-name' = 'order_user',
        |   'username' = 'root',
        |   'password' = '12345678'
        |)
        |""".stripMargin)

    // Sink
    tableEnv.executeSql(
      """
        |insert into mysql_order_user
        |select id, name, age, city
        |from kafka_order_user /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
        |""".stripMargin)

    Thread.sleep(Integer.MAX_VALUE)
  }
}
