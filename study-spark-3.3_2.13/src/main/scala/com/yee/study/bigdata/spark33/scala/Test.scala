package com.yee.study.bigdata.spark33.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
 *
 * @author Roger.Yi
 */
object Test {
  def main(args: Array[String]) {
    val inputFile =  "study-spark-3.3_2.13/data/wordcount.csv"

    test1(inputFile)
    test2(inputFile)
  }

  def test2(file: String): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(file)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }

  def test1(file: String): Unit = {
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
}
