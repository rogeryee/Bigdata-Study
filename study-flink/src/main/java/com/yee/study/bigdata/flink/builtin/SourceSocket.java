package com.yee.study.bigdata.flink.builtin;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 通过 socketTextStream 方式来读取数据的
 *
 * nc -lk 6789
 *
 * @author Roger.Yi
 */
public class SourceSocket {

    public static void main(String[] args) throws Exception {
        // 获取执行环境对象 StreamExecutionEnvironment
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 加载数据源获取数据抽象对象
        DataStreamSource<String> sourceDataStream = executionEnvironment.socketTextStream("localhost", 6789);

        // 执行逻辑处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = sourceDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        }).setParallelism(3);

        // 分组聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneDS.keyBy("0").sum(1).setParallelism(1);

        // 输出结果
        resultDS.print().setParallelism(5);

        // 提交执行
        executionEnvironment.execute("StreamingWordCount running.");
    }
}
