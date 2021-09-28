package com.yee.study.bigdata.spark.scala.wordcount

import java.net.URL

import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * 计算单词个数的示例
  *
  * 本例使用了三种不同的方式来计算：
  * 1. Scala
  * 2. Spark RDD
  * 3. Spark DataFrame
  *
  * java-study-spark/data/wordcount.csv文件
  * 内容如下：
  * Shanghai,Beijin,Tianjin,Hongkong
  * Roger,Johnny,Jason,Joe,
  * Phoebe,Roger,Beijin,NewYork
  * Paris,Phoebe,Roger,Joe
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "/Users/RogerYee/MyWork/DevTools/hadoop-2.7.3")
    
    val file = "java-study-spark/data/wordcount.csv"

    val spark = SparkSession
      .builder()
      .appName("WorkCountSample")
      .master("local[*]")
      .getOrCreate()

    /**
      * 采用Scala实现
      */
    wordCountByScala(file)

    /**
      * 采用Spark RDD实现
      */
    wordCountBySparkRDD(spark, file)

    /**
      * 采用Spark DataFrame实现
      */
    wordCountBySparkDataFrame(spark, file)
  }

  /**
    * 采用Scala实现WordCount
    *
    * @param file
    */
  def wordCountByScala(file: String): Unit = {
    val bufferedSource = Source.fromFile(file)
    val text = bufferedSource
      .getLines()
      .toArray
      .flatMap(line => line.split(","))
      .map(word => (word, 1))
      .groupBy(_._1)
      .map(value => (value._1, value._2.map(cell => cell._2).sum))

    println("------ Implementation by Scala:")
    text.foreach(println)
  }

  /**
    * 采用Spark RDD实现WordCount
    *
    * @param spark
    * @param file
    */
  def wordCountBySparkRDD(spark: SparkSession, file: String): Unit = {

    val rdd = spark
      .sparkContext
      .textFile(file)
      .flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("------ Implementation by Spark RDD:")
    rdd.collect().foreach(println)
  }

  /**
    * 采用Spark DataFrame实现WordCount
    *
    * @param spark
    * @param file
    */
  def wordCountBySparkDataFrame(spark: SparkSession, file: String): Unit = {

    println("------ Implementation by Spark DataFrame:")
    
    val df = spark
      .read
      .format("CSV")
      .option("header", "false")
      .load(file)
      .toDF("col1", "col2", "col3", "col4")

    df.show()
    df.printSchema()

    df.select("col1")
      .union(df.select("col2"))
      .union(df.select("col3"))
      .union(df.select("col4"))
      .toDF("word")
      .groupBy("word")
      .count()
        .show(100)
  }
}


