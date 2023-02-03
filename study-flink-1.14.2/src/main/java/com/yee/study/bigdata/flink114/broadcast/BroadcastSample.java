package com.yee.study.bigdata.flink114.broadcast;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * broadcast 广播
 * <p>
 * a join b ==> datastream1 join datastream2
 * datastream1.map(value => {1、先拿到全量的 datastream2 的数据， 2、拿value到datastream2中去匹配})
 *
 * @author Roger.Yi
 */
public class BroadcastSample {

    public static void main(String[] args) throws Exception {
        // ENV
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // a join b ==> datastream1 join datastream2
        // datastream1.map(value => {1、先拿到全量的 datastream2 的数据， 2、拿value到datastream2中去匹配})
        // 1. Prepare broadcast data
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("zs", 18));
        data.add(new Tuple2<>("ls", 20));
        data.add(new Tuple2<>("ww", 17));
        data.add(new Tuple2<>("pp", 32));
        DataSet<Tuple2<String, Integer>> ds1 = env.fromCollection(data);

        DataSource<String> ds2 = env.fromElements("zs", "ls", "ww", "xx");

        DataSet<String> result = ds2.map(new RichMapFunction<String, String>() {

            Map<String, Integer> broadcastData = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                List<Tuple2<String, Integer>> broadcastData = getRuntimeContext().getBroadcastVariable("broadcastData");
                broadcastData.forEach(data -> {
                    this.broadcastData.put(data.f0, data.f1);
                });
            }

            @Override
            public String map(String value) throws Exception {
                Integer age = this.broadcastData.get(value);
                return age == null ? null : value + " - " + age;
            }
        }).withBroadcastSet(ds1, "broadcastData").filter(new FilterFunction<String>() {
            // 过滤匹配不上的数据（null）
            @Override
            public boolean filter(String value) throws Exception {
                return value != null;
            }
        });

        result.print();

//        env.execute("Broadcast");
    }
}
