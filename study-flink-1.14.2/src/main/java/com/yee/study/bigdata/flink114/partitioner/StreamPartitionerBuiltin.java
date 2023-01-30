package com.yee.study.bigdata.flink114.partitioner;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Stream 的 8中分区策略
 *
 * @author Roger.Yi
 */
public class StreamPartitionerBuiltin {
    public static void main(String[] args) {
        // ENV
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // nc -lk 6789
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        // 1. 伸缩：1 => N 或者 N => 1 基于上下游Operator的并行度
        source.rescale();

        // 2. 重新平均分区：均匀，Round-Robin算法
        source.rebalance();

        // 3. KeyBy 根据字段来hash分区，相同的key在同一分区内
        source.keyBy(1);

        // 4. 所有数据发给下游第一个 Task
        source.global();

        // 5. 上下游的本地 Task 映射
        // 上下游两个 Operator 之间能否链接成 Operator Chain，需要满足9个条件
        source.forward();

        // 6. 随机分区：根据均匀分布随机分配元素
        source.shuffle();

        // 7. 广播：向每个分区广播数据
        source.broadcast();
    }
}
