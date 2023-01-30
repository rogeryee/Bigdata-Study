package com.yee.study.bigdata.flink114.transform;

import com.yee.study.bigdata.flink114.source.userdefine.LongSourceWithoutParallel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * connect + CoMap 算子
 *
 * @author Roger.Yi
 */
public class TransformConnectCoMap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ds1 = (1, 2, 3, ...)
        DataStreamSource<Long> ds1 = env.addSource(new LongSourceWithoutParallel(1, 1000)).setParallelism(1);

        // ds2 = (1000, 1001, 1002, ...)
        // strDs = (str_1000, str_1001, str_1002, ...)
        DataStreamSource<Long> ds2 = env.addSource(new LongSourceWithoutParallel(1000, 3000)).setParallelism(1);
        SingleOutputStreamOperator<String> strDs = ds2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_" + value;
            }
        });

        // connect 链接： 结果数据流的 数据类型是： 元组（第一个流的泛型,  第二个流的泛型）
        ConnectedStreams<Long, String> connectedStreams = ds1.connect(strDs);

        // 设置2个Stream的输出结果
        SingleOutputStreamOperator<Object> resultDs = connectedStreams.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value) throws Exception {
                return value * 2;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value + "_s";
            }
        });

        resultDs.print().setParallelism(1);

        env.execute("TransformConnectCoMap");
    }
}
