package com.yee.study.bigdata.spark.scala.json

import org.apache.spark.sql.DataFrame

/**
 *
 * @author Roger.Yi
 */
object JsonSample {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .master("local[*]").appName("test")
      .getOrCreate();
    //提供隐式转换功能，比如将 Rdd 转为 dataframe
    import spark.implicits._

    val df: DataFrame = spark.sparkContext.parallelize(Array(("abc", 2), ("efg", 4))).toDF()
    df.show()
  }
}
