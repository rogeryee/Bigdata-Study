package com.yee.study.bigdata.flink114.sample;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 写入 ElasticSearch7 示例
 * <p>
 * Flink 1.14 后，已经摈弃了 FlinkKafkaConsumer/FlinkKafkaProducer，改为了 KafkaSource 和 KafkaSink
 * <p>
 * 1. kafka中创建 test-upstream 主题
 * ./kafka-topics.sh --create --topic test-upstream --bootstrap-server localhost:9092
 * <p>
 * 3. 创建 producer 向 test-upstream 中发送消息
 * ./kafka-console-producer.sh --topic test-upstream --bootstrap-server localhost:9092
 *
 * @author Roger.Yi
 */
public class ElasticsearchSample {

    public static void main(String[] args) throws Exception {
        // 获取执行环境对象 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取Kafka消息
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setTopics("test-events")
                .setGroupId("flink")
                .setBootstrapServers("localhost:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "2000")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 计算
        SingleOutputStreamOperator<Doc> resultDS = ds.map(new MapFunction<String, Doc>() {
            @Override
            public Doc map(String s) throws Exception {
                String[] array = s.split(",");
                return new Doc(array[0], array[1]);
            }
        });

        // 输出到控制台
        resultDS.print();

        // 输出到 elasticsearch
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.28.133.72", 9204, "http"));
        httpHosts.add(new HttpHost("10.28.133.73", 9204, "http"));

        ElasticsearchSink.Builder<Doc> esSinkBuilder = new ElasticsearchSink.Builder<Doc>(
                httpHosts,
                new ElasticsearchSinkFunction<Doc>() {
                    public IndexRequest createIndexRequest(Doc element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element.getData());
                        json.put("id", element.getId());
                        return Requests.indexRequest()
                                       .index("tptb-test-index")
                                       .id(element.getId())
                                       .source(json);
                    }

                    @Override
                    public void process(Doc element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        // provide a RestClientFactory for custom configuration on the internally created REST client
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setRequestConfigCallback(
                            builder -> builder.setConnectTimeout(10000).setSocketTimeout(300000));
                    restClientBuilder.setHttpClientConfigCallback(
                            builder -> {
                                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                                credentialsProvider.setCredentials(
                                        AuthScope.ANY,
                                        new UsernamePasswordCredentials("tptb", "sT5j9nJ^7Zz8"));
                                builder.setDefaultCredentialsProvider(credentialsProvider);
                                return builder;
                            });
                });

        // finally, build and add the sink to the job's pipeline
        resultDS.addSink(esSinkBuilder.build());

        env.execute("Kafka Sample");
    }
}

class Doc {
    private String id;
    private String data;

    public Doc(String id, String data) {
        this.id = id;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Doc{" +
                "id='" + id + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}