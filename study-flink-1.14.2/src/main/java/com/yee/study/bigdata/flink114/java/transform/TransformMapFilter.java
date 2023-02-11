package com.yee.study.bigdata.flink114.java.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Map_Filter 算子测试
 *
 * @author Roger.Yi
 */
public class TransformMapFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

        ArrayList<Integer> data = new ArrayList<>();
        data.add(1);
        data.add(2);
        data.add(3);
        data.add(4);
        DataStreamSource<Integer> dataDS = executionEnvironment.fromCollection(data);

        // map element
        SingleOutputStreamOperator<Integer> dataStream = dataDS.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println("接受到了数据：" + value);
                return value;
            }
        });

        // filter element
        SingleOutputStreamOperator<Integer> filterDS = dataStream.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer number) throws Exception {
                return number % 2 == 0;
            }
        });

        filterDS.print().setParallelism(1);

        executionEnvironment.execute("TransformationMapFilter");
    }
}
