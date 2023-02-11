package com.yee.study.bigdata.flink114.java.sample;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Flink 消费kafka以及生产数据到kafka 示例
 * <p>
 * Flink 1.14 后，已经摈弃了 FlinkKafkaConsumer/FlinkKafkaProducer，改为了 KafkaSource 和 KafkaSink
 * <p>
 * 1. kafka中创建 test-upstream 主题
 * ./kafka-topics.sh --create --topic test-upstream --bootstrap-server localhost:9092
 * <p>
 * 2. kafka中创建 test-downstream 主题
 * ./kafka-topics.sh --create --topic test-downstream --bootstrap-server localhost:9092
 * <p>
 * 3. 创建 producer 向 test-upstream 中发送消息
 * ./kafka-console-producer.sh --topic test-upstream --bootstrap-server localhost:9092
 * <p>
 * 4. 创建 consumer 向 test-downstream 中发送消息
 * ./kafka-console-consumer.sh --topic test-downstream --from-beginning --bootstrap-server localhost:9092
 * <p>
 * 注意：
 * Flink中通过 setStartingOffsets 来设置 offset 的获取方式
 * 1） Start from committed offset of the consuming group, without reset strategy
 * .setStartingOffsets(OffsetsInitializer.committedOffsets())
 * <p>
 * 2）Start from committed offset, also use EARLIEST as reset strategy if committed offset doesn't exist
 * .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
 * <p>
 * 3）Start from the first record whose timestamp is greater than or equals a timestamp (milliseconds)
 * .setStartingOffsets(OffsetsInitializer.timestamp(1657256176000L))
 * <p>
 * 4）Start from earliest offset
 * .setStartingOffsets(OffsetsInitializer.earliest())
 * <p>
 * 5）Start from latest offset
 * .setStartingOffsets(OffsetsInitializer.latest())
 *
 * @author Roger.Yi
 */
public class KafkaSample {

    public static void main(String[] args) throws Exception {
        // 获取执行环境对象 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取Kafka消息
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setTopics("test-upstream")
                .setGroupId("flink")
                .setBootstrapServers("localhost:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "1000")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 计算
        SingleOutputStreamOperator<String> resultDS = ds.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return "flink => " + s;
            }
        });

        // 输出到控制台
        resultDS.print();

        // 输出到Kakfa
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                                                   .setTopic("test-downstream")
                                                                   .setValueSerializationSchema(new SimpleStringSchema())
                                                                   .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        resultDS.sinkTo(sink);

        env.execute("Kafka Sample");
    }
}
