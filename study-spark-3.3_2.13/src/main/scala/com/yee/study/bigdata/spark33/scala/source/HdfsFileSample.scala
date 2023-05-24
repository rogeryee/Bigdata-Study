package com.yee.study.bigdata.spark33.scala.source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取 hdfs 上的 person.json 文件
 *
 * 注：上传person.json文件
 * 1) cd ~/MyWork/yee/bigdata-study/study-spark-3.3_2.13/data
 * 2）hadoop fs -mkdir /yish
 * 3）hadoop fs -put person.json /yish
 *
 * @author Roger.Yi
 */
object HdfsFileSample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HdfsFileSample")
      .master("local[*]")
      .getOrCreate()

    val dataFrame: DataFrame = spark.read.json("hdfs://localhost:9000/yish/person.json")
    dataFrame.show()
    spark.close()
  }
}
