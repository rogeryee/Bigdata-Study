package com.yee.study.bigdata.spark33.scala.sql

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable


/**
 * Spark sql 示例
 *
 * @author Roger.Yi
 */
object SqlSample {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("ExternalSourceSample")
      .master("local[*]")
      .getOrCreate()

    sample_1(spark)

    spark.close()
  }

  /**
   * 1）筛选出符合条件(城市,平台，版本)的数据
   * 2）统计每天的uv排名前3的搜索词
   *
   * @param spark
   */
  def sample_1(spark: SparkSession): Unit = {
    val initDataRdd: RDD[String] = spark.sparkContext.textFile("study-spark-3.3_2.13/data/city.txt")
    val initRowRdd: RDD[Row] = initDataRdd
      .map(
        line => {
          val logdata: Array[String] = line.split(" ")
          val date: String = logdata(0)
          val userName: String = logdata(1)
          val keyWord: String = logdata(2)
          val city: String = logdata(3)
          val phone: String = logdata(4)
          val version: String = logdata(5)
          Row(date, userName, keyWord, city, phone, version)
        })

    val dataTypes = StructType(
      Array(
        StructField("date", DataTypes.StringType, true),
        StructField("user_name", DataTypes.StringType, true),
        StructField("keyword", DataTypes.StringType, true),
        StructField("city", DataTypes.StringType, true),
        StructField("phone", DataTypes.StringType, true),
        StructField("version", DataTypes.StringType, true)
      )
    )

    val logDF: DataFrame = spark.createDataFrame(initRowRdd, dataTypes)
    logDF.createTempView("log_data")

    spark.sql(
      """
        | with base as (
        |    select date, keyword, count(distinct user_name) uv
        |    from log_data
        |    where version in ('1.0', '1.2', '1.5')
        |    group by date, keyword
        | )
        | select
        | *
        | from (
        |     select *
        |          , row_number() over (partition by date order by uv desc) as rank
        |     from base
        | ) t
        | where t.rank <= 3
        | order by date, rank desc
        |""".stripMargin)
      .show
  }
}