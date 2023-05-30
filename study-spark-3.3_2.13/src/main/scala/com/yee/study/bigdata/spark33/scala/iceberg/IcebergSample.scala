package com.yee.study.bigdata.spark33.scala.iceberg

import org.apache.spark.sql.SparkSession

/**
 * Spark 读写 Iceberg 示例
 *
 * @author Roger.Yi
 */
object IcebergSample {

  def main(args: Array[String]): Unit = {
    //    sample_1() // 基于 HDFS
    //    sample_2() // 基于 HDFS (基于默认的 spark_catalog)
    sample_3() // 基于 Hive
  }

  /**
   * 基于 HDFS
   *
   * @param spark
   */
  def sample_1(): Unit = {
    val spark: SparkSession = getSparkSession()
    try {
      // create table
      spark.sql(
        """
          |create table hadoop_local.iceberg.employee_iceberg
          |(
          | id int,
          | name string,
          | age int
          |) using iceberg
          |""".stripMargin)

      // insert data
      spark.sql(
        """
          |insert into hadoop_local.iceberg.employee_iceberg values
          | (1,"zs",18),
          | (2,"ls",19),
          | (3,"ww",20)
          |""".stripMargin)

      // query data
      spark.sql(
        """
          |select *
          |from hadoop_local.iceberg.employee_iceberg
          |where age >= 20
          |""".stripMargin)
        .show
    } finally {
      // Clean up
      spark.close()
    }
  }

  /**
   * 基于 HDFS (基于默认的 spark_catalog)
   *
   * @param spark
   */
  def sample_2(): Unit = {
    val spark: SparkSession = getSparkSessionDefault()
    try {
      // create table
      spark.sql(
        """
          |create table iceberg.employee_iceberg_v2
          |(
          | id int,
          | name string,
          | age int
          |) using iceberg
          |""".stripMargin)

      // insert data
      spark.sql(
        """
          |insert into iceberg.employee_iceberg_v2 values
          | (1,"zs",18),
          | (2,"ls",19),
          | (3,"ww",20)
          |""".stripMargin)

      // query data
      spark.sql(
        """
          |select *
          |from iceberg.employee_iceberg_v2
          |where age >= 20
          |""".stripMargin)
        .show
    } finally {
      // Clean up
      spark.close()
    }
  }

  /**
   * 基于 Hive
   *
   * @param spark
   */
  def sample_3(): Unit = {
    val spark: SparkSession = getSparkSessionWithHiveSupport()

    val tableName = "roger_tmp.employee_iceberg_v3"
    try {
      // create table
      spark.sql(
        s"""
          |create table ${tableName}
          |(
          | id int,
          | name string,
          | age int
          |) using iceberg
          |""".stripMargin)

      // insert data
      spark.sql(
        s"""
          |insert into ${tableName}
          | (1,"zs",18),
          | (2,"ls",19),
          | (3,"ww",20)
          |""".stripMargin)

      // query data
      spark.sql(
        s"""
          |select *
          |from ${tableName}
          |where age >= 20
          |""".stripMargin)
        .show
    } finally {
      // Clean up
      spark.close()
    }
  }

  /**
   * 获取 SparkSession（基于HDFS）
   *
   * @return
   */
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("DeltaLakeSample")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.hadoop_local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_local.type", "hadoop")
      .config("spark.sql.catalog.hadoop_local.warehouse", "hdfs://localhost:9000/yish")
      .getOrCreate()
  }

  /**
   * 获取 SparkSession（基于HDFS）
   *
   * @return
   */
  def getSparkSessionDefault(): SparkSession = {
    SparkSession
      .builder()
      .appName("DeltaLakeSample")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://localhost:9000/yish")
      .getOrCreate()
  }

  /**
   * 获取 SparkSession（基于Hive）
   *
   * @return
   */
  def getSparkSessionWithHiveSupport(): SparkSession = {
    SparkSession
      .builder()
      .appName("DeltaLakeSample")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.spark_catalog.uri", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()
  }
}