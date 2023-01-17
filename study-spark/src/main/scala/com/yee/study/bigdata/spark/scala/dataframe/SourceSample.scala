package com.yee.study.bigdata.spark.scala.dataframe

import org.apache.spark.sql.SparkSession

/**
 *
 * @author Roger.Yi
 */
object SourceSample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SourceSample")
      .master("local[*]")
      .getOrCreate()

//    val spark = SparkSession.builder().getOrCreate();

    println("spark here")

    spark.stop()
  }
}
