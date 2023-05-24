package com.yee.study.bigdata.spark33.scala.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


/**
 * Spark sql 读写外部数据源的示例
 *
 * 读数据格式：
 * DataFrameReader.format(...).option("key", "value").schema(...).load()
 *
 * 示例：
 * spark.read.format("csv")
 * .option("mode", "FAILFAST")          // 读取模式
 * .option("inferSchema", "true")       // 是否自动推断 schema
 * .option("path", "path/to/file(s)")   // 文件路径
 * .schema(someSchema)                  // 使用预定义的 schema
 * .load()
 *
 * 读取模式有以下三种可选项：
 * 1）permissive	当遇到损坏的记录时，将其所有字段设置为 null，并将所有损坏的记录放在名为 _corruption t_record 的字符串列中
 * 2）dropMalformed	删除格式不正确的行
 * 3）failFast	遇到格式不正确的数据时立即失败
 *
 * 写数据格式：
 * DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save()
 *
 * 示例：
 * dataframe.write.format("csv")
 * .option("mode", "OVERWRITE")         //写模式
 * .option("dateFormat", "yyyy-MM-dd")  //日期格式
 * .option("path", "path/to/file(s)")
 * .save()
 *
 * 写数据模式有以下四种可选项：
 * 1) SaveMode.ErrorIfExists	如果给定的路径已经存在文件，则抛出异常，这是写数据默认的模式
 * 2) SaveMode.Append	数据以追加的方式写入
 * 3) SaveMode.Overwrite	数据以覆盖的方式写入
 * 4) SaveMode.Ignore	如果给定的路径已经存在文件，则不做任何操作
 *
 * @author Roger.Yi
 */
object ExternalSourceSample {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("ExternalSourceSample")
      .master("local[*]")
      .getOrCreate()

    //    sample_1(spark)
    //    sample_2(spark)
    sample_3(spark)

    spark.close()
  }

  /**
   * CSV 操作
   *
   * @param spark
   */
  def sample_1(spark: SparkSession): Unit = {
    val schema = new StructType(Array(
      StructField("deptno", LongType, nullable = false),
      StructField("dname", StringType, nullable = true),
      StructField("loc", StringType, nullable = true)
    ))
    val df = spark.read.format("csv")
      .option("mode", "FAILFAST")
      .schema(schema)
      .load("study-spark-3.3_2.13/data/person.csv")
    df.show()

    // 写入CSV，并指定分隔符
    df.write.format("csv").mode("overwrite").option("sep", "\t").save("study-spark-3.3_2.13/data/person_write.csv")
  }

  /**
   * Parquet 操作
   *
   * 采用追加的方式写入
   *
   * @param spark
   */
  def sample_2(spark: SparkSession): Unit = {
    spark.read.format("json").load("study-spark-3.3_2.13/data/person.json")
      .write.mode(SaveMode.Append).format("parquet")
      .save("study-spark-3.3_2.13/data/person_parquet")
  }

  /**
   * 加载 Parquet格式文件
   */
  def sample_3(spark: SparkSession): Unit = {
    // 方式1
    spark
      .read
      .format("parquet")
      .load("study-spark-3.3_2.13/data/person_parquet")
      .show

    // 方式2
    spark
      .read
      .parquet("study-spark-3.3_2.13/data/person_parquet")
      .show
  }
}