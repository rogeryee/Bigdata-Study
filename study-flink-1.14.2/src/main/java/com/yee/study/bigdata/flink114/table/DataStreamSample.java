package com.yee.study.bigdata.flink114.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * DataStream 和 Table 配合使用的示例1
 * DataStream 获取Socket的数据
 * <p>
 * nc -lk 6789
 *
 * @author Roger.Yi
 */
public class DataStreamSample {

    public static void main(String[] args) throws Exception {
        // Stream env
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // Table env
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(sEnv);

        // Socket Source and operator
        DataStreamSource<String> socketDS = sEnv.socketTextStream("localhost", 6789);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCntDs = socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(",");
                Stream.of(words).forEach(word -> {
                    out.collect(Tuple2.of(word, 1));
                });
            }
        });

        // Convert DataStream to Table
        Table wordTable = tabEnv.fromDataStream(wordCntDs).as("word", "state");
        tabEnv.createTemporaryView("wordTable", wordTable);
        Table resultTable = tabEnv.sqlQuery("select word, count(1) as cnt from wordTable group by word");

        // resultTable 一直在更新，这里需要穿换成 ChangelogStream
//        tabEnv.toChangelogStream(resultTable).print();
        resultTable.execute().print();

        sEnv.execute("");
    }
}
