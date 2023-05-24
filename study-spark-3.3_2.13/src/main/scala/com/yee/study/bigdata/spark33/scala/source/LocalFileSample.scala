package com.yee.study.bigdata.spark33.scala.source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取本地文件 person.json 文件示例
 *
 * @author Roger.Yi
 */
object LocalFileSample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("LocalFileSample")
      .master("local[*]")
      .getOrCreate()

    val dataFrame: DataFrame = spark.read.json("study-spark-3.3_2.13/data/person.json")
    dataFrame.show()
    spark.close()
  }
}
