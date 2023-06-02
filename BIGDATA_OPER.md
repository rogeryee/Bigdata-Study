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

```shell
```





# Flink SQL

```shell
./bin/sql-client.sh


```

