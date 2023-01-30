package com.yee.study.bigdata.flink114.sink.builtin;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * print sink 测试
 */
public class SinkPrint {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<Integer, String, Double>> sourceDS = executionEnvironment.fromElements(
                Tuple3.of(19, "xuzheng", 178.8), Tuple3.of(17, "huangbo", 168.8), Tuple3.of(18, "wangbaoqiang", 174.8),
                Tuple3.of(18, "liujing", 195.8), Tuple3.of(18, "liutao", 182.7), Tuple3.of(21, "huangxiaoming", 184.8)
                                                                                                      );

        // print
        sourceDS.print().setParallelism(1);

        // printToErr
        sourceDS.printToErr().setParallelism(1);

        executionEnvironment.execute("SinkPrint");
    }
}
