package com.yee.study.bigdata.flink.sample;

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

/**
 * Flink 消费kafka 以及 Offset 示例
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
 *
 * @author Roger.Yi
 */
public class KafkaConsumerSample {

    public static void main(String[] args) throws Exception {
        // 获取执行环境对象 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取Kafka消息
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setTopics("test-events")
                .setGroupId("flink")
                .setBootstrapServers("localhost:9092")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("enable.auto.commit", "false")
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
        env.execute("Kafka Sample");
    }
}
