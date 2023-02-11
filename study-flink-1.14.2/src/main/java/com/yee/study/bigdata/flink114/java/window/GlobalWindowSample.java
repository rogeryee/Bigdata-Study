package com.yee.study.bigdata.flink114.java.window;

import com.yee.study.bigdata.flink114.java.window.support.WordSplitFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

/**
 * Global Window 示例
 * <p>
 * 需求： 单词每出现三次统计一次
 *
 * @author Roger.Yi
 */
public class GlobalWindowSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        // Operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds = source
                .flatMap(new WordSplitFunction())
                .keyBy(tuple -> tuple.f0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(3))
                .sum(1);

        // Sink
        ds.print().setParallelism(1);

        // run
        env.execute("TimeWindowSample");
    }
}
