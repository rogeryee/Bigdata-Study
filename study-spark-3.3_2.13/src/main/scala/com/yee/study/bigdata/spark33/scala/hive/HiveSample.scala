package com.yee.study.bigdata.spark33.scala.hive

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Spark 读写 Hive 示例
 *
 * @author Roger.Yi
 */
object HiveSample {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("ExternalSourceSample")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    // 读取表数据
    // create table roger_tmp.test_tbl as select 1 id, 'Roger' name, 'SH' city;
    spark.sql(
      """
        |select *
        |from roger_tmp.test_tbl
        |""".stripMargin)
      .show

    // 写入数据方式1：DataFrame.write
    import spark.implicits._
    val employeeDF = Seq(
      (1, "James", 30, "M", 2022),
      (2, "Ann", 40, "F", 2023),
      (3, "Jeff", 41, "M", 2022),
      (4, "Jennifer", 20, "F", 2023)
    ).toDF("id", "name", "age", "gender", "dt")

    employeeDF
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("dt")
      .saveAsTable("roger_tmp.employee")

    spark.sql(
      """
        |select *
        |from roger_tmp.employee
        |where dt = 2023
        |""".stripMargin)
      .show

    // 写入数据方式2：Create table + Insert
    import spark.implicits._
    val tableCreationSql =
      s"""
         |create table if not exists roger_tmp.employee_v2
         |(
         |  `id` int,
         |  `name` string,
         |  `age` int,
         |  `gender` string
         |)
         |partitioned by (dt bigint)
         |stored as parquet
       """.stripMargin
    spark.sql(tableCreationSql)

    val overwriteSql =
      s"""
         |insert overwrite table roger_tmp.employee_v2
         |select * from roger_tmp.employee
      """.stripMargin
    spark.sql(overwriteSql)

    spark.close()
  }
}