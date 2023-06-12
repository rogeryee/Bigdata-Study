# Hadoop

```shell
cd /Users/cntp/MyWork/DevTools/hadoop-3.3.1/sbin

./start-all.sh
./stop-all.sh

hadoop fs -ls 
```



# Hive

```shell
cd /Users/cntp/MyWork/DevTools/apache-hive-3.1.2-bin/bin

./hive --service metastore
./hive --service hiveserver2
./beeline -u jdbc:hive2://localhost:10000

# error Permission denied: user=anonymous, access=EXECUTE, inode="/user":cntp:supergroup:drwx------
hdfs dfs -chmod 777 /user
```





# Kafka

## 服务启动

```shell
./zookeeper-server-start.sh ../config/zookeeper.properties

./kafka-server-start.sh ../config/server.properties
```



## 基本操作

```shell
# 查看Topic列表
./kafka-topics.sh --list --bootstrap-server localhost:9092

# 删除Topic
./kafka-topics.sh --delete --topic user_order --bootstrap-server localhost:9092

# 创建Topic
./kafka-topics.sh --create --topic  user-behavior  --bootstrap-server localhost:9092

# 查看Topic信息
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic user-behavior

# 查看消费者状态和消费详情
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# 查看消费者状态和消费详情
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group test-group --describe

# 创建生产者和消费者
./kafka-console-producer.sh --topic kafka_to_flink --bootstrap-server localhost:9092
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka_to_flink
```



# Flink SQL

```shell
# 本地单机模式启动
./start-cluster.sh
./bin/sql-client.sh
```



# 实例

## flink sql 示例

- MySQL 结果表

```SQL
-- MySQL
-- 订单用户
create table flink.order_user (
    id varchar(32),
    name varchar(32),
    age int,
    city varchar(32),
    mobile varchar(32),
    PRIMARY KEY (`id`)
);

-- 订单用户统计（按照所在城市）
create table flink.order_user_stat (
    city varchar(32),
    user_cnt bigint,
    avg_age int,
    PRIMARY KEY (`city`)
);
```

- Hive 表

```sql
create table roger_tmp.user_extra as 
select '1' id, '13100001234' mobile union all
select '2' id, '13100001235' mobile union all
select '3' id, '13100001236' mobile union all
select '4' id, '13100001237' mobile
```

- Flink SQL

```SQL
-- CATALOG
CREATE CATALOG hive_catalog WITH (
    'type' = 'hive',
    'default-database' = 'roger_tmp',
    'hive-conf-dir' = '/Users/cntp/MyWork/DevTools/apache-hive-3.1.2-bin/conf'
);

USE CATALOG hive_catalog;

-- Kafka Source Table
-- 1,Joey,22,SH
-- 2,John,30,SH
-- 3,Roger,20,BJ
-- 4,Andy,21,SZ
-- 5,Adams,31,CZ
-- 3,Roger1,30,BJ
-- 6,Phoebe,20,SH
create table hive_catalog.roger_tmp.kafka_order_user (
    id STRING,
    name STRING,
    age integer,
    city STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp'
) with (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.auto.commit.interval.ms' = '1000',
    'properties.enable.auto.commit' = 'true',
    'properties.group.id' = 'flink_group',
    'topic' = 'order-user',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true'
);

-- MySQL 结果表
CREATE TABLE hive_catalog.roger_tmp.mysql_order_user (
    id STRING,
    name STRING,
    age INT,
    city STRING,
    mobile STRING,
    primary key (id) not enforced
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/flink?useSSL=false',
   'table-name' = 'order_user',
   'username' = 'root',
   'password' = '12345678'
);

CREATE TABLE hive_catalog.roger_tmp.mysql_order_user_stat (
    city STRING,
    user_cnt BIGINT,
    avg_age INT,
    primary key (city) not enforced
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/flink?useSSL=false',
   'table-name' = 'order_user_stat',
   'username' = 'root',
   'password' = '12345678'
);

insert into mysql_order_user 
select 
   ou.id, ou.name, ou.age, ou.city, ue.mobile 
from kafka_order_user /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ ou
left join user_extra ue on ue.id = ou.id;
```

- Flink SQL (Upsert)
```SQL
set 'sql-client.execution.result-mode' = 'tableau’;

-- SQL Client
create catalog hive_catalog WITH (
    'type' = 'hive',
    'default-database' = 'roger_tmp',
    'hive-conf-dir' = '/Users/cntp/MyWork/DevTools/apache-hive-3.1.2-bin/conf'
);

create catalog iceberg_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://localhost:9083',
  'hive-conf-dir'='/Users/cntp/MyWork/DevTools/apache-hive-3.1.2-bin/conf',
  'warehouse'='hdfs://localhost:9000/user/hive/warehouse'
);

CREATE TABLE iceberg_catalog.roger_tmp.sample (
  `id` BIGINT NOT NULL,
  `data` STRING,
  PRIMARY KEY (`id`) NOT ENFORCED
) PARTITIONED BY (`id`)
WITH ('format-version'='2', 'write.upsert.enabled'='true');

INSERT INTO iceberg_catalog.roger_tmp.sample VALUES (1, '2023');
INSERT INTO iceberg_catalog.roger_tmp.sample VALUES (2, '2022'); 
INSERT INTO iceberg_catalog.roger_tmp.sample VALUES (1, '2024'); -- Upsert table
```
