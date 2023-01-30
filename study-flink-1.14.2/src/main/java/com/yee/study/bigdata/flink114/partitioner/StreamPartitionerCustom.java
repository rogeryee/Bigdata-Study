package com.yee.study.bigdata.flink114.partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义 Partitioner
 *
 * @author Roger.Yi
 */
public class StreamPartitionerCustom {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // nc -lk 6789
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        DataStream<String> result = source.partitionCustom(new CustomPartitioner(), word -> word);

        result.print().setParallelism(1);

        env.execute("StreamPartitionerCustom");
    }

    // 自定义一个 分区器
    public static class CustomPartitioner implements Partitioner<String> {
        @Override
        public int partition(String key, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
