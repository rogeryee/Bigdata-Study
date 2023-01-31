[TOC]

```shell
./flink run-application -t yarn-application \
-c com.yee.study.bigdata.flink1142.sample.ElasticsearchSample \
/Users/cntp/MyWork/yee/bigdata-study/study-flink-1.14.2/target/bigdata-flink-1.14.2-1.0-SNAPSHOT.jar

./flink list -t yarn-application -Dyarn.application.id=application_1674971692755_0009

./flink cancel -t yarn-application -Dyarn.application.id=application_1674971692755_0009 1197a34ed993cfa0ef92ea890d4c8d9b

# 删除Topic
./kafka-topics.sh --delete --topic test-events --bootstrap-server localhost:9092

# 创建Topic
./kafka-topics.sh --create --topic test-events --bootstrap-server localhost:9092 --partitions 3

# 查看Topic列表
./kafka-topics.sh --list --bootstrap-server localhost:9092

# 查看Topic信息
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic test-events

# 查看消费者状态和消费详情
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# 查看消费者状态和消费详情
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group test-events-consumer-group --describe

./kafka-console-producer.sh --topic test-events --bootstrap-server localhost:9092
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --consumer.config ../config/consumer_test_events.properties --topic test-events --offset earliest --partition 0
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --consumer.config ../config/consumer_test_events.properties --topic test-events --offset earliest --partition 2


./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group test-events-consumer-group

./kafka-console-consumer.sh --bootstrap-server localhost:9092 --consumer.config ../config/consumer_test_events.properties --topic test-events
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --consumer.config ../config/consumer_test_events.properties --topic test-events --partition 0
```

