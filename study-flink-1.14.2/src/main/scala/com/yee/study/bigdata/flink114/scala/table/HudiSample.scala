package com.yee.study.bigdata.flink114.scala.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, FormatDescriptor, Schema, TableDescriptor}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 *
 * @author Roger.Yi
 */
object HudiSample {

  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(sEnv)

    // Source
    tEnv.createTemporaryTable(
      "csv_sample",
      TableDescriptor
        .forConnector("filesystem")
        .schema(
          Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("name", DataTypes.STRING())
            .column("age", DataTypes.INT())
            .column("partition", DataTypes.STRING())
            .build())
        .option("path", "file:///Users/cntp/MyWork/yee/bigdata-study/study-flink-1.14.2/data/table_csv_sample.csv")
        .format(
          FormatDescriptor.forFormat("csv")
            .option("field-delimiter", ",")
            .build())
        .build())

    tEnv.executeSql(
      """
        |CREATE TABLE hudi_table (
        |  id VARCHAR(20) PRIMARY KEY NOT ENFORCED,
        |  name VARCHAR(10),
        |  age INT,
        |  `partition` VARCHAR(20)
        |)
        |PARTITIONED BY (`partition`)
        |WITH (
        |  'connector' = 'hudi',
        |  'path' = '/cntp/hudi/table',
        |  'table.type' = 'MERGE_ON_READ'
        |)
        |
        |""".stripMargin)

    // Sink
    tEnv.executeSql(
      """
        |INSERT INTO hudi_table
        |SELECT `id`, `name`, `age`, `partition`
        |FROM csv_sample
        |""".stripMargin)

    sEnv.execute()
  }
}
