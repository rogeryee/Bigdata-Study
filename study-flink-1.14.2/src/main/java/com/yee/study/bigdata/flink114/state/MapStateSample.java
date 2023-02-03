package com.yee.study.bigdata.flink114.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

/**
 * MapState 示例
 *
 * 需求：每 3 个相同 key 就输出他们的 平均值
 *
 * @author Roger.Yi
 */
public class MapStateSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);

        // Source
        DataStreamSource<Tuple2<Long, Long>> sourceDS = environment.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(1L, 5L), Tuple2.of(2L, 3L), Tuple2.of(2L, 5L));

        // Operator
        // MapState 存储数据集合，注意，一个 Key 一个 MapState
        SingleOutputStreamOperator<Tuple2<Long, Double>> resultDS = sourceDS.keyBy(0)
                .flatMap(new CountAverageWithMapState());

        resultDS.print();
        environment.execute("MapStateSample");
    }

    static class CountAverageWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

        private MapState<String, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("map_state", String.class, Long.class);
            mapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> record, Collector<Tuple2<Long, Double>> collector) throws Exception {
            // 记录状态数据
            mapState.put(UUID.randomUUID().toString(), record.f1);
            // 执行判断
            Iterable<Long> values = mapState.values();
            ArrayList<Long> dataList = Lists.newArrayList(values);

            int count = 3;
            if (dataList.size() == count) {
                long total = 0;
                for (Long data : dataList) {
                    total += data;
                }
                double avg = (double) total / count;
                collector.collect(Tuple2.of(record.f0, avg));
                mapState.clear();
            }
        }
    }
}
