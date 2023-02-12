package com.yee.study.bigdata.flink114.scala.table

import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, FormatDescriptor, Schema, TableDescriptor, TableEnvironment}

/**
 * 基于 Flink Table，读取csv内容并输出到新的csv的示例 (基于Scala)
 *
 * @author Roger.Yi
 */
object CsvScalaSample {

  def main(args: Array[String]): Unit = {
    val envSettings = EnvironmentSettings.newInstance().inBatchMode().build()
    val tabEnv = TableEnvironment.create(envSettings)

    // Source
    tabEnv.createTemporaryTable(
      "csv_sample",
      TableDescriptor
        .forConnector("filesystem")
        .schema(
          Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("name", DataTypes.STRING())
            .column("age", DataTypes.INT())
            .build())
        .option("path", "file:///Users/cntp/MyWork/yee/bigdata-study/study-flink-1.14.2/data/table_csv_sample.csv")
        .format(
          FormatDescriptor.forFormat("csv")
            .option("field-delimiter", ",")
            .build())
        .build());

    val csv_sample = tabEnv.sqlQuery(
      """
        |select
        |   id
        | , name
        | , age
        |from csv_sample
        |where age > 25
        |""".stripMargin)

    /**
     * (
     * `id` BIGINT,
     * `name` STRING,
     * `age` INT)
     */
    csv_sample.printSchema()

    // Sink
    tabEnv.createTemporaryTable(
      "csv_sample_sink",
      TableDescriptor
        .forConnector("print")
        .schema(
          Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("name", DataTypes.STRING())
            .column("age", DataTypes.INT())
            .build())
        .build());

    /**
     * 3> +I[103, wangwu, 28]
     * 7> +I[104, laoyou, 30]
     * 1> +I[105, laowang, 35]
     */
    csv_sample.executeInsert("csv_sample_sink");
  }
}
