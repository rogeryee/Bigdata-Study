package com.yee.study.bigdata.spark.scala.json

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, struct, to_json}


/**
 *
 * @author Roger.Yi
 */
object JsonSample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]").appName("test")
      .getOrCreate();

    import spark.implicits._
    val df: DataFrame = spark.sparkContext.parallelize(Array(
      ("hol_001", "ins_001", "张三", "F"),
      ("hol_001", "ins_002", "李四", "F"),
      ("hol_002", "ins_003", "王五", "M"),
      ("hol_003", "ins_004", "赵六", "M"))).toDF("hol_id", "ins_id", "ins_name", "ins_gender")
    df.show()

    val jsonStr: String = df.toJSON.collectAsList.toString
    println(jsonStr)

//    val df2: DataFrame = df.groupBy("hol_id").agg(collect_list(struct("insured_id", col("ins_id"), "ins_name", "ins_gender"))).alias("ins_body")
    val df2: DataFrame = df.groupBy("hol_id").agg(collect_list(struct("ins_id", "ins_name", "ins_gender"))).alias("ins_body")
    df2.show()

//    println(df2.toJSON.collectAsList().toString)
  }
}
