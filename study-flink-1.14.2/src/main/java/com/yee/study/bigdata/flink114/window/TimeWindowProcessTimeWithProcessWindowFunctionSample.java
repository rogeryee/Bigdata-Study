package com.yee.study.bigdata.flink114.window;

import com.yee.study.bigdata.flink114.window.support.SumProcessFunction;
import com.yee.study.bigdata.flink114.window.support.WordSplitFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * ProcessWindowFunction 示例
 * <p>
 * 需求：每隔 5秒 计算最近 10秒 单词出现的次数
 *
 * @author Roger.Yi
 */
public class TimeWindowProcessTimeWithProcessWindowFunctionSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        // Operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds = source
                .flatMap(new WordSplitFunction())
                .keyBy(tuple -> tuple.f0)
                // 每隔 5s 计算过去 10s内 数据的结果。用的是 ProcessingTime
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time
                        .seconds(5)))
                .process(new SumProcessFunction());

        // Sink
        ds.print().setParallelism(1);

        // run
        env.execute("TimeWindowWithFunction");
    }
}
