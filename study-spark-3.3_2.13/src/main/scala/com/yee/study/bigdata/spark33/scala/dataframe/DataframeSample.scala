package com.yee.study.bigdata.spark33.scala.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * DataFrame 和 RDD 示例
 *
 * @author Roger.Yi
 */
object DataframeSample {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("DataframeSample")
      .master("local[*]")
      .getOrCreate()

    sample_1(spark)
    sample_2(spark)
    sample_3(spark)

    spark.close()
  }

  /**
   * 示例1 加载person.json数据，dataframe基本操作
   *
   * @param spark
   */
  def sample_1(spark: SparkSession): Unit = {
    val dataFrame: DataFrame = spark.read.json("study-spark-3.3_2.13/data/person.json")
    dataFrame.show()

    // 打印元数据信息
    dataFrame.printSchema()

    // 查询指定的列
    dataFrame.select(dataFrame.col("name")).show()

    // 对于分数加一操作
    dataFrame.select(dataFrame.col("sorce").plus(1)).show()
    dataFrame.groupBy(dataFrame.col("id")).count().show()
  }

  /**
   * RDD 与 DataFrame互相转化示例
   *
   * 本例基于POJO
   *
   * @param spark
   */
  def sample_2(spark: SparkSession): Unit = {
    val rdd: RDD[String] = spark.sparkContext.textFile("study-spark-3.3_2.13/data/person.txt")

    import spark.implicits._

    // RDD -> Dataframe
    val personDF: DataFrame = rdd
      .map(_.split(" "))
      .map(item => Student(item(0).toInt, item(1).trim, item(2).toInt))
      .toDF()

    personDF.show

    // DataFrame -> RDD
    // 临时视图
    personDF.createTempView("person")
    val filterDF: DataFrame = spark.sql("select * from person where score >= 100")
    val filterRdd: RDD[Row] = filterDF.rdd
    filterRdd.cache()

    filterRdd
      .map(row => Student(row.getInt(0), row.getString(1), row.getInt(2)))
      .foreach(println)
  }

  /**
   * RDD 与 DataFrame互相转化示例
   *
   * 通过StructType动态转换
   *
   * @param spark
   */
  def sample_3(spark: SparkSession): Unit = {
    val initDataRdd: RDD[String] = spark.sparkContext.textFile("study-spark-3.3_2.13/data/person.txt")
    val initRowRdd: RDD[Row] = initDataRdd
      .map(line => {
        val fields: Array[String] = line.split(" ")
        Row(fields(0).toInt, fields(1).trim, fields(2).toInt)
      })

    val dataTypes = StructType(
      Array(
        StructField("id", DataTypes.IntegerType, true),
        StructField("name", DataTypes.StringType, true),
        StructField("score", DataTypes.IntegerType, true),
      )
    )

    val personDF: DataFrame = spark.createDataFrame(initRowRdd, dataTypes)
    personDF.show
  }
}

case class Student(id: Int, name: String, score: Int)