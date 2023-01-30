package com.yee.study.bigdata.flink.sink.userdefine;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 自定义Sink
 *
 * @author Roger.Yi
 */
public class SinkPrintSink {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<Tuple3<Integer, String, Double>> sourceDS = env.fromElements(
                Tuple3.of(19, "xuzheng", 178.8), Tuple3.of(17, "huangbo", 168.8), Tuple3.of(18, "wangbaoqiang", 174.8),
                Tuple3.of(18, "liujing", 195.8), Tuple3.of(18, "liutao", 182.7), Tuple3.of(21, "huangxiaoming", 184.8));

        // SinkFunction
        sourceDS.addSink(new SinkFunction<Tuple3<Integer, String, Double>>() {
            @Override
            public void invoke(Tuple3<Integer, String, Double> value, Context context) throws Exception {
                System.out.println("第一种实现: " + value);
            }
        });

        // RichSinkFunction
        sourceDS.addSink(new RichSinkFunction<Tuple3<Integer, String, Double>>() {
            @Override
            public void invoke(Tuple3<Integer, String, Double> value, Context context) throws Exception {
                System.out.println("第二种实现：" + value);
            }
        });

        env.execute("SinkPrintSink");
    }
}
