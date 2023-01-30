package com.yee.study.bigdata.flink.source.userdefine;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义的 不带并行度 的数据源
 * Source Operator 的 Task 只有一个！
 * 1、单并行度： MySource implements SourceFunction<T>
 * 2、多并行度： MySource implements ParallelSourceFunction<T>
 *
 * @author Roger.Yi
 */
public class SourceNoParallel {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 这个数据源：有节奏的每隔 1s 输出一个顺序递增的自然数
        DataStreamSource<Long> numberDS = executionEnvironment.addSource(new LongSourceWithoutParallel(1, 1000))
                .setParallelism(1);

        // 先map一下，再filter一下
        SingleOutputStreamOperator<Long> resultDS = numberDS.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到了数据：" + value);
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        }).setParallelism(2);

        // 输出
        resultDS.print().setParallelism(2);

        executionEnvironment.execute("SourceNoParallel");
    }
}