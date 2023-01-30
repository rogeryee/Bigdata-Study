package com.yee.study.bigdata.flink114.source.builtin;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 从本地集合读取数据，进行计算
 */
public class SourceCollection {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

        // 本地集合
        ArrayList<String> data = new ArrayList<>();
        data.add("huangbo");
        data.add("xuzheng");
        data.add("wangbaoqiang");
        data.add("shenteng");

        DataStreamSource<String> dataSourceDS = executionEnvironment.fromCollection(data);

        // 计算
        SingleOutputStreamOperator<String> resultDS = dataSourceDS.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return "nx_" + s;
            }
        });

        // 输出
        resultDS.print();

        executionEnvironment.execute("SourceCollection running.");
    }
}