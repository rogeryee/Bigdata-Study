package com.yee.study.bigdata.flink114.java.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * AggregationFunction 增量聚合示例
 * <p>
 * 求窗口内数据的平均数
 *
 * @author Roger.Yi
 */
public class AggregationFunctionSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        // map
        SingleOutputStreamOperator<Integer> intDataStream = source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        });

        intDataStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                     .aggregate(new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                         @Override
                         public Tuple2<Integer, Integer> createAccumulator() {
                             return Tuple2.of(0, 0);
                         }

                         @Override
                         public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                             return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                         }

                         @Override
                         public Double getResult(Tuple2<Integer, Integer> accumulator) {
                             return (double) accumulator.f0 / accumulator.f1;
                         }

                         @Override
                         public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                             return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                         }
                     }).print();

        // run
        env.execute("AggregationFunction");
    }
}
