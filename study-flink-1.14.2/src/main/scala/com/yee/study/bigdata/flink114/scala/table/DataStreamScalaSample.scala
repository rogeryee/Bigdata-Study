package com.yee.study.bigdata.flink114.scala.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

/**
 * DataStream 和 Table 配合使用的示例1 (基于Scala)
 * DataStream 获取Socket的数据
 *
 * nc -lk 6789
 *
 * @author Roger.Yi
 */
object DataStreamScalaSample {

  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(sEnv)

    // Source
    val dataStream = sEnv.socketTextStream("localhost", 6789)

    // Operation
    val wordCntDs = dataStream.flatMap((str: String, out: Collector[(String, Int)]) => {
      str.split(",").foreach(s => out.collect(Tuple2(s, 1)))
    })

    // Convert DataStream to Table
    val wordTable = tEnv.fromDataStream(wordCntDs).as("word", "state");
    tEnv.createTemporaryView("wordTable", wordTable);
    val resultTable = tEnv.sqlQuery("select word, count(1) as cnt from wordTable group by word");

    // Sink
    resultTable.execute().print();
    sEnv.execute("");
  }
}

