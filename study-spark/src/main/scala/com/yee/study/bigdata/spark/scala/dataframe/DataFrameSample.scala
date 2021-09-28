package com.yee.study.bigdata.spark.scala.dataframe

import java.sql.Date

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Spark SQL示例
  *
  * 展示了5中使用 Data Frame的方法
  */
object DataFrameSample {

  def main(args: Array[String]): Unit = {
    val input = "java-study-spark/data/dataframe.csv"
    val spark = SparkSession
      .builder()
      .appName("DataFrameSample")
      .master("local[*]")
      .getOrCreate()

    // 从文件中读取数据并创建 Data Frame
    //    useDataFrameByFile(spark, input)

    // 从文件中读取数据并创建 Temp View
    //    useDataFrameByView(spark, input)

    // 手动构建Schema，并创建 Data Frame
//    useDataFrameByRowStructure(spark, input)

    // 使用隐式转换创建 Data Frame
//    useDataFrameByImplicit(spark, input)

    // 使用Case类配合创建 Data Frame
    useDataFrameByCaseClassImplicit(spark, input)
  }

  /**
    * 从文件中读取数据并创建 Data Frame
    *
    * @param spark
    * @param input
    */
  def useDataFrameByFile(spark: SparkSession, input: String): Unit = {

    println("------ DataFrame by File:")

    val df = spark
      .read
      .format("CSV")
      .option("header", "true")
      .load(input)

    df.printSchema()

    df.show()

    df.select("company", "ipo").show()
  }

  /**
    * 从文件中读取数据并创建 Temp View
    *
    * @param spark
    * @param input
    */
  def useDataFrameByView(spark: SparkSession, input: String): Unit = {

    println("------ DataFrame by Temp View:")

    spark
      .read
      .csv(input)
      .createOrReplaceTempView("company")

    val df = spark
      .sql("select * from company where _c3 = 1")

    df.printSchema()

    df.show()
  }

  /**
    * 手动构建Schema，并创建 Data Frame
    *
    * @param spark
    * @param input
    */
  def useDataFrameByRowStructure(spark: SparkSession, input: String): Unit = {

    println("------ DataFrame by Row Structure:")

    val rdd = spark
      .sparkContext
      .parallelize(Seq(
        Row(1, "FirstValue", java.sql.Date.valueOf("2018-09-01")),
        Row(2, "SecondValue", java.sql.Date.valueOf("2018-09-02"))
      ))

    val schema = StructType(List(
      StructField("integre_column", IntegerType, nullable = false),
      StructField("string_column", StringType, nullable = true),
      StructField("date_column", DateType, nullable = true)
    ))

    val df = spark
      .createDataFrame(rdd, schema)

    df.printSchema()

    df.show()
  }

  /**
    * 使用隐式转换创建 Data Frame
    *
    * @param spark
    * @param input
    */
  def useDataFrameByImplicit(spark: SparkSession, input: String): Unit = {

    println("------ DataFrame by Implicit:")

    import spark.sqlContext.implicits._

    val df = Seq(
      (1, "FirstValue", java.sql.Date.valueOf("2018-09-01")),
      (2, "SecondValue", java.sql.Date.valueOf("2018-09-02"))
    ).toDF("id", "value", "crtDate")

    df.printSchema()

    df.show()
  }

  /**
    * 使用Case类配合创建 Data Frame
    *
    * @param spark
    * @param input
    */
  def useDataFrameByCaseClassImplicit(spark: SparkSession, input: String): Unit = {

    println("------ DataFrame by CaseClass Implicit:")

    import spark.sqlContext.implicits._

    val df = Seq(
      Sample(1, "FirstValue", java.sql.Date.valueOf("2018-08-01")),
      Sample(2, "SecondValue", java.sql.Date.valueOf("2018-08-02"))
    ).toDF("id", "value", "crtDate")

    df.printSchema()

    df.show()
  }
}

case class Sample(index: Int, value: String, crtDate: Date)
