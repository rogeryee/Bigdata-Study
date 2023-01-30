package com.yee.study.bigdata.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * ReduceFunction 增量聚合示例
 *
 * @author Roger.Yi
 */
public class ReduceFunctionSample {

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
                     .reduce(new ReduceFunction<Integer>() {
                         @Override
                         public Integer reduce(Integer value1, Integer value2) throws Exception {
                             System.out.println("Reduce : v1 = " + value1 + ", v2 = " + value2);
                             return value1 + value2;
                         }
                     }).print();

        // run
        env.execute("ReduceFunctionSample");
    }
}
