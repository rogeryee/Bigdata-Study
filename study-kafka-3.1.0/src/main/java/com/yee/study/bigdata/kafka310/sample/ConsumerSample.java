package com.yee.study.bigdata.kafka310.sample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka Consumer 示例
 * <p>
 * 1）创建 Topic，并指定3个分区
 * ./kafka-topics.sh --create --topic test-events --bootstrap-server localhost:9092 --partitions 3
 *
 * @author Roger.Yi
 */
public class ConsumerSample {

    public static void main(String[] args) throws InterruptedException {
        // 不确认消息offset
//        consumerNoOffsetCommit();

        // 确认消息offset
        consumerWithOffsetCommit();
    }

    // 创建一个消费者，不确认offset enable.auto.commit=false
    // 指定某个Topic 和 指定某个Partition 只能二选一
    public static void consumerNoOffsetCommit() throws InterruptedException {
        // 1.创建Kafka消费者配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("auto.offset.reset", "earliest"); // earliest/latest/none 默认是 latest
        props.setProperty("enable.auto.commit", "false"); // 自动提交offset，默认提交为 true
        props.setProperty("group.id", "test-grp-1"); // consumer group

        // 拉取的key、value数据的
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2.创建Kafka消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // 3. 订阅要消费的主题
        // 指定消费者从哪个topic中拉取数据
        kafkaConsumer.subscribe(Collections.singletonList("test-events"));
        // 指定某个Topic的某个分区
//        kafkaConsumer.assign(Collections.singletonList(new TopicPartition("test-events", 0)));

        // 4.使用一个while循环，不断从Kafka的topic中拉取消息
        while (true) {
            // Kafka的消费者一次拉取一批的数据
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                String topic = consumerRecord.topic();
                long offset = consumerRecord.offset();
                String key = consumerRecord.key();
                String value = consumerRecord.value();

                System.out.println("topic: " + topic + " partition:" + consumerRecord.partition() + " offset:" + offset + " key:" + key + " value:" + value);
            }
            Thread.sleep(1000);
        }
    }

    // 创建一个消费者，确认offset enable.auto.commit=true
    // earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
    // latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
    // none topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常(Undefined offset with no reset policy for partition xxx)
    public static void consumerWithOffsetCommit() throws InterruptedException {
        // 1.创建Kafka消费者配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("auto.offset.reset", "earliest"); // earliest/latest/none 默认是 latest
        props.setProperty("enable.auto.commit", "true"); // 自动提交offset，默认提交为 true
        props.setProperty("auto.commit.interval.ms", "1000"); // 自动提交offset的时间间隔
        props.setProperty("group.id", "test-grp-1"); // consumer group

        // 拉取的key、value数据的
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2.创建Kafka消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // 3. 订阅要消费的主题
        // 指定消费者从哪个topic中拉取数据
        kafkaConsumer.subscribe(Collections.singletonList("test-events"));

        // 4.使用一个while循环，不断从Kafka的topic中拉取消息
        while (true) {
            // Kafka的消费者一次拉取一批的数据
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                String topic = consumerRecord.topic();
                long offset = consumerRecord.offset();
                String key = consumerRecord.key();
                String value = consumerRecord.value();

                System.out.println("topic: " + topic + " partition:" + consumerRecord.partition() + " offset:" + offset + " key:" + key + " value:" + value);
            }
            Thread.sleep(1000);
        }
    }
}
